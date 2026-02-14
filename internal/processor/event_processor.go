package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"db-stream/internal/config"
	"db-stream/pkg/models"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// DebeziumEvent represents a Debezium CDC event from Kafka
type DebeziumEvent struct {
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
	Source SourceInfo             `json:"source"`
	Op     string                 `json:"op"` // c=create, u=update, d=delete, r=read
	TsMs   int64                  `json:"ts_ms"`
}

// SourceInfo contains Debezium source metadata
type SourceInfo struct {
	Table string `json:"table"`
	DB    string `json:"db"`
}

// DDLEvent represents a DDL event
type DDLEvent struct {
	ID           string    `json:"id"`
	EventType    string    `json:"event_type"`
	SchemaName   string    `json:"schema_name"`
	TableName    string    `json:"table_name"`
	DDLStatement string    `json:"ddl_statement"`
	ExecutedAt   time.Time `json:"executed_at"`
}

// EventProcessor handles processing of CDC events
type EventProcessor struct {
	config         *config.Config
	kafkaReader    *kafka.Reader
	ddlReader      *kafka.Reader
	clickhouseConn driver.Conn
	logger         *zap.Logger
	running        bool
	mu             sync.RWMutex
	batchBuffer    []*models.CDCEvent
	ddlBuffer      []*DDLEvent
	batchSize      int
	flushInterval  time.Duration
}

// NewEventProcessor creates a new event processor
func NewEventProcessor(cfg *config.Config, logger *zap.Logger) (*EventProcessor, error) {
	// Initialize ClickHouse connection
	chConn, err := initClickHouseConnection(&cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize ClickHouse: %w", err)
	}

	// Initialize Kafka reader for CDC events
	kafkaReader := initKafkaReader(&cfg.Kafka)

	// Initialize Kafka reader for DDL events
	ddlReader := initDDLKafkaReader(&cfg.Kafka)

	processor := &EventProcessor{
		config:         cfg,
		kafkaReader:    kafkaReader,
		ddlReader:      ddlReader,
		clickhouseConn: chConn,
		logger:         logger,
		running:        false,
		batchBuffer:    make([]*models.CDCEvent, 0),
		ddlBuffer:      make([]*DDLEvent, 0),
		batchSize:      10,
		flushInterval:  2 * time.Second,
	}

	return processor, nil
}

// Start begins processing events from Kafka
func (p *EventProcessor) Start(ctx context.Context) error {
	p.logger.Info("Starting event processor")

	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("event processor is already running")
	}
	p.running = true
	p.mu.Unlock()

	// Create ClickHouse tables if they don't exist
	if err := p.ensureClickHouseTables(ctx); err != nil {
		return fmt.Errorf("failed to ensure ClickHouse tables: %w", err)
	}

	// Start batch flush goroutine
	go p.batchFlushLoop(ctx)

	// Start DDL batch flush goroutine
	go p.ddlFlushLoop(ctx)

	p.logger.Info("Starting to consume from Kafka", zap.String("cdc_topic", p.config.Kafka.Topics.CDC), zap.String("ddl_topic", p.config.Kafka.Topics.DDL))

	// Start consuming CDC messages in a goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := p.processCDCMessage(ctx); err != nil {
					if ctx.Err() != nil {
						return
					}
					p.logger.Error("Error processing CDC message", zap.Error(err))
				}
			}
		}
	}()

	// Start consuming DDL messages in the main loop
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Event processor stopped")
			return nil
		default:
			if err := p.processDDLMessage(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				p.logger.Error("Error processing DDL message", zap.Error(err))
			}
		}
	}
}

// ddlFlushLoop periodically flushes the DDL batch
func (p *EventProcessor) ddlFlushLoop(ctx context.Context) {
	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.Lock()
			if len(p.ddlBuffer) > 0 {
				if err := p.flushDDLBatchUnsafe(ctx); err != nil {
					p.logger.Error("Error flushing DDL batch", zap.Error(err))
				}
			}
			p.mu.Unlock()
		}
	}
}

// processDDLMessage processes a single DDL message from Kafka
func (p *EventProcessor) processDDLMessage(ctx context.Context) error {
	msg, err := p.ddlReader.FetchMessage(ctx)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
			return nil
		}
		return fmt.Errorf("failed to fetch DDL message: %w", err)
	}

	p.logger.Info("Received DDL message",
		zap.Int64("offset", msg.Offset),
		zap.ByteString("value", msg.Value[:min(200, len(msg.Value))]),
		zap.String("topic", msg.Topic),
	)

	var ddlEvent DDLEvent
	if err := json.Unmarshal(msg.Value, &ddlEvent); err != nil {
		p.logger.Warn("Could not parse DDL message", zap.Error(err))
	} else {
		p.mu.Lock()
		p.ddlBuffer = append(p.ddlBuffer, &ddlEvent)
		shouldFlush := len(p.ddlBuffer) >= p.batchSize
		p.mu.Unlock()

		if shouldFlush {
			if err := p.flushDDLBatch(ctx); err != nil {
				p.logger.Error("Error flushing DDL batch", zap.Error(err))
			}
		}
	}

	if err := p.ddlReader.CommitMessages(ctx, msg); err != nil {
		p.logger.Warn("Error committing DDL message", zap.Error(err))
	}

	return nil
}

// flushDDLBatch flushes the current DDL batch to ClickHouse
func (p *EventProcessor) flushDDLBatch(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flushDDLBatchUnsafe(ctx)
}

// flushDDLBatchUnsafe flushes DDL batch without acquiring mutex
func (p *EventProcessor) flushDDLBatchUnsafe(ctx context.Context) error {
	if len(p.ddlBuffer) == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		p.logger.Debug("DDL Batch processed",
			zap.Int("size", len(p.ddlBuffer)),
			zap.Duration("duration", time.Since(start)))
	}()

	if err := p.storeDDLBatchInClickHouse(ctx, p.ddlBuffer); err != nil {
		return fmt.Errorf("failed to store DDL batch in ClickHouse: %w", err)
	}

	p.ddlBuffer = p.ddlBuffer[:0]

	return nil
}

// storeDDLBatchInClickHouse stores a batch of DDL events in ClickHouse
func (p *EventProcessor) storeDDLBatchInClickHouse(ctx context.Context, events []*DDLEvent) error {
	batch, err := p.clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO ddl_events (
			id, event_type, schema_name, table_name, ddl_statement, executed_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare DDL batch: %w", err)
	}

	for _, event := range events {
		if err := batch.Append(
			event.ID,
			event.EventType,
			event.SchemaName,
			event.TableName,
			event.DDLStatement,
			event.ExecutedAt,
		); err != nil {
			return fmt.Errorf("failed to append DDL event to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send DDL batch: %w", err)
	}

	return nil
}

// Shutdown gracefully stops the event processor
func (p *EventProcessor) Shutdown(ctx context.Context) error {
	p.logger.Info("Shutting down event processor")

	p.mu.Lock()
	p.running = false
	p.mu.Unlock()

	// Flush remaining batches
	if len(p.batchBuffer) > 0 {
		if err := p.flushBatch(ctx); err != nil {
			p.logger.Error("Error flushing final CDC batch", zap.Error(err))
		}
	}

	if len(p.ddlBuffer) > 0 {
		if err := p.flushDDLBatch(ctx); err != nil {
			p.logger.Error("Error flushing final DDL batch", zap.Error(err))
		}
	}

	// Close Kafka readers
	if p.kafkaReader != nil {
		if err := p.kafkaReader.Close(); err != nil {
			p.logger.Error("Error closing Kafka reader", zap.Error(err))
		}
	}

	if p.ddlReader != nil {
		if err := p.ddlReader.Close(); err != nil {
			p.logger.Error("Error closing DDL Kafka reader", zap.Error(err))
		}
	}

	// Close ClickHouse connection
	if p.clickhouseConn != nil {
		if err := p.clickhouseConn.Close(); err != nil {
			p.logger.Error("Error closing ClickHouse connection", zap.Error(err))
		}
	}

	p.logger.Info("Event processor shutdown completed")
	return nil
}

// processCDCMessage processes a single CDC message from Kafka
func (p *EventProcessor) processCDCMessage(ctx context.Context) error {
	// Use FetchMessage instead of ReadMessage for more control
	msg, err := p.kafkaReader.FetchMessage(ctx)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded || ctx.Err() == context.Canceled {
			return nil
		}
		return fmt.Errorf("failed to fetch message: %w", err)
	}

	p.logger.Info("Received message",
		zap.Int64("offset", msg.Offset),
		zap.ByteString("value", msg.Value[:min(200, len(msg.Value))]),
		zap.String("topic", msg.Topic),
	)

	// Try to parse as Debezium event first
	var debeziumEvent DebeziumEvent
	if err := json.Unmarshal(msg.Value, &debeziumEvent); err == nil {
		p.logger.Debug("Parsed as Debezium event",
			zap.String("table", debeziumEvent.Source.Table),
			zap.String("op", debeziumEvent.Op),
		)
		// It's a Debezium event, convert to our format
		event := p.convertDebeziumEvent(&debeziumEvent)
		p.mu.Lock()
		p.batchBuffer = append(p.batchBuffer, event)
		shouldFlush := len(p.batchBuffer) >= p.batchSize
		p.mu.Unlock()

		if shouldFlush {
			if err := p.flushBatch(ctx); err != nil {
				p.logger.Error("Error flushing batch", zap.Error(err))
			}
		}
	} else {
		p.logger.Warn("Could not parse message as Debezium event", zap.Error(err))
	}

	// Commit the message
	if err := p.kafkaReader.CommitMessages(ctx, msg); err != nil {
		p.logger.Warn("Error committing message", zap.Error(err))
	}

	return nil
}

// convertDebeziumEvent converts a Debezium event to our CDCEvent format
func (p *EventProcessor) convertDebeziumEvent(debezium *DebeziumEvent) *models.CDCEvent {
	// Determine operation
	operation := "READ"
	switch debezium.Op {
	case "c":
		operation = "INSERT"
	case "u":
		operation = "UPDATE"
	case "d":
		operation = "DELETE"
	case "r":
		operation = "READ"
	}

	// Get data (after for insert/update, before for delete)
	data := debezium.After
	oldData := debezium.Before

	// If no "after" for snapshot, use "before" data
	if data == nil && debezium.Op == "r" {
		data = debezium.Before
	}

	// Serialize data to JSON strings
	dataJSON, _ := json.Marshal(data)
	oldDataJSON, _ := json.Marshal(oldData)

	// Get timestamp
	timestamp := time.UnixMilli(debezium.TsMs)

	return &models.CDCEvent{
		ID:        fmt.Sprintf("%s-%d-%d", debezium.Source.Table, debezium.TsMs, len(p.batchBuffer)),
		Table:     debezium.Source.Table,
		Operation: operation,
		Data:      string(dataJSON),
		OldData:   string(oldDataJSON),
		Timestamp: timestamp,
	}
}

// batchFlushLoop periodically flushes the batch
func (p *EventProcessor) batchFlushLoop(ctx context.Context) {
	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.Lock()
			if len(p.batchBuffer) > 0 {
				if err := p.flushBatchUnsafe(ctx); err != nil {
					p.logger.Error("Error flushing batch", zap.Error(err))
				}
			}
			p.mu.Unlock()
		}
	}
}

// flushBatch flushes the current batch to ClickHouse
func (p *EventProcessor) flushBatch(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flushBatchUnsafe(ctx)
}

// flushBatchUnsafe flushes batch without acquiring mutex
func (p *EventProcessor) flushBatchUnsafe(ctx context.Context) error {
	if len(p.batchBuffer) == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		p.logger.Debug("Batch processed",
			zap.Int("size", len(p.batchBuffer)),
			zap.Duration("duration", time.Since(start)))
	}()

	// Store in ClickHouse
	if err := p.storeBatchInClickHouse(ctx, p.batchBuffer); err != nil {
		return fmt.Errorf("failed to store batch in ClickHouse: %w", err)
	}

	// Clear buffer
	p.batchBuffer = p.batchBuffer[:0]

	return nil
}

// storeBatchInClickHouse stores a batch of events in ClickHouse
func (p *EventProcessor) storeBatchInClickHouse(ctx context.Context, events []*models.CDCEvent) error {
	// Prepare batch insert query
	batch, err := p.clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO cdc_events (
			id, table, operation, data, old_data, timestamp
		) VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	// Add events to batch
	for _, event := range events {
		if err := batch.Append(
			event.ID,
			event.Table,
			event.Operation,
			event.Data,
			event.OldData,
			event.Timestamp,
		); err != nil {
			return fmt.Errorf("failed to append event to batch: %w", err)
		}
	}

	// Send batch
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// ensureClickHouseTables creates ClickHouse tables if they don't exist
func (p *EventProcessor) ensureClickHouseTables(ctx context.Context) error {
	// Create main CDC events table
	if err := p.clickhouseConn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS cdc_events (
			id String,
			table String,
			operation String,
			data String,
			old_data String,
			timestamp DateTime64(3)
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (timestamp, table, id)
		TTL toDate(timestamp) + toIntervalDay(30)
	`); err != nil {
		return fmt.Errorf("failed to create cdc_events table: %w", err)
	}

	// Create materialized view for daily metrics
	if err := p.clickhouseConn.Exec(ctx, `
		CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_daily_metrics
		ENGINE = SummingMergeTree()
		PARTITION BY toYYYYMM(date)
		ORDER BY (date, table, operation)
		AS SELECT
			toDate(timestamp) as date,
			table,
			operation,
			count() as event_count
		FROM cdc_events
		GROUP BY date, table, operation
	`); err != nil {
		return fmt.Errorf("failed to create daily metrics view: %w", err)
	}

	// Create DDL events table
	if err := p.clickhouseConn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS ddl_events (
			id String,
			event_type String,
			schema_name String,
			table_name String,
			ddl_statement String,
			executed_at DateTime64(3)
		) ENGINE = MergeTree()
		ORDER BY (executed_at, event_type)
	`); err != nil {
		return fmt.Errorf("failed to create ddl_events table: %w", err)
	}

	p.logger.Info("ClickHouse tables ensured successfully")
	return nil
}

// initClickHouseConnection initializes ClickHouse connection
func initClickHouseConnection(cfg *config.ClickHouseConfig) (driver.Conn, error) {
	options := clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Protocol:     clickhouse.HTTP,
		DialTimeout:  30 * time.Second,
		MaxOpenConns: cfg.MaxOpenConns,
		MaxIdleConns: cfg.MaxIdleConns,
	}

	conn, err := clickhouse.Open(&options)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}

	// Test connection
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return conn, nil
}

// initKafkaReader initializes Kafka reader
func initKafkaReader(cfg *config.KafkaConfig) *kafka.Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  cfg.GroupID + "-processor",
		Topic:    cfg.Topics.CDC,
		MinBytes: 1,    // 1 byte
		MaxBytes: 10e6, // 10MB
		// Debug:    true,
		StartOffset: kafka.FirstOffset,
	})

	return reader
}

// initDDLKafkaReader initializes Kafka reader for DDL events
func initDDLKafkaReader(cfg *config.KafkaConfig) *kafka.Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		GroupID:     cfg.GroupID + "-processor-ddl",
		Topic:       cfg.Topics.DDL,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})

	return reader
}
