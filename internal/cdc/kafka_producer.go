package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"db-stream/internal/config"
	"db-stream/pkg/models"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// KafkaProducer handles publishing CDC events to Kafka
type KafkaProducer struct {
	config   *config.KafkaConfig
	writer   *kafka.Writer
	logger   *zap.Logger
	producer *kafka.Writer
}

// NewKafkaProducer creates a new Kafka producer for CDC events
func NewKafkaProducer(cfg *config.KafkaConfig, logger *zap.Logger) (*KafkaProducer, error) {
	if err := validateKafkaConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid kafka config: %w", err)
	}

	// Create Kafka writer
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  cfg.Topics.CDC,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           parseAcks(cfg.Acks),
		Compression:            parseCompression(cfg.CompressionType),
		BatchSize:              cfg.BatchSize,
		BatchTimeout:           parseDuration(cfg.FlushFrequency),
		AllowAutoTopicCreation: true,
	}

	producer := &KafkaProducer{
		config:   cfg,
		writer:   writer,
		logger:   logger,
		producer: writer,
	}

	return producer, nil
}

// HandleEvent implements EventHandler interface for single events
func (kp *KafkaProducer) HandleEvent(ctx context.Context, event *models.CDCEvent) error {
	return kp.handleSingleEvent(ctx, event)
}

// HandleBatch implements EventHandler interface for batch processing
func (kp *KafkaProducer) HandleBatch(ctx context.Context, events []*models.CDCEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Create Kafka messages
	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		message, err := kp.createKafkaMessage(event)
		if err != nil {
			kp.logger.Error("Failed to create Kafka message",
				zap.String("event_id", event.ID),
				zap.Error(err))
			continue
		}
		messages = append(messages, message)
	}

	if len(messages) == 0 {
		return nil
	}

	// Write messages to Kafka
	if err := kp.producer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("failed to write messages to Kafka: %w", err)
	}

	kp.logger.Debug("Successfully published events to Kafka",
		zap.Int("count", len(messages)),
		zap.String("topic", kp.config.Topics.CDC))

	return nil
}

// Close closes the Kafka producer
func (kp *KafkaProducer) Close() error {
	if kp.producer != nil {
		if err := kp.producer.Close(); err != nil {
			kp.logger.Error("Failed to close Kafka producer", zap.Error(err))
			return err
		}
		kp.logger.Info("Kafka producer closed successfully")
	}
	return nil
}

// handleSingleEvent handles a single CDC event
func (kp *KafkaProducer) handleSingleEvent(ctx context.Context, event *models.CDCEvent) error {
	message, err := kp.createKafkaMessage(event)
	if err != nil {
		return fmt.Errorf("failed to create Kafka message: %w", err)
	}

	return kp.producer.WriteMessages(ctx, message)
}

// createKafkaMessage creates a Kafka message from a CDC event
func (kp *KafkaProducer) createKafkaMessage(event *models.CDCEvent) (kafka.Message, error) {
	// Serialize event to JSON
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("failed to marshal event: %w", err)
	}

	// Determine topic based on source and table
	topic := kp.getTopicForEvent(event)

	// Create message
	message := kafka.Message{
		Topic: topic,
		Key:   []byte(event.ID),
		Value: eventBytes,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(event.Operation)},
			{Key: "source_table", Value: []byte(event.Table)},
			{Key: "timestamp", Value: []byte(event.Timestamp.Format(time.RFC3339))},
			{Key: "connector_type", Value: []byte("postgresql")},
		},
		Time: event.Timestamp,
	}

	return message, nil
}

// getTopicForEvent determines the Kafka topic for an event
func (kp *KafkaProducer) getTopicForEvent(event *models.CDCEvent) string {
	// Use topic pattern: {base-topic}.{source}.{table}
	if event.Table != "" {
		return fmt.Sprintf("%s.postgresql.%s", kp.config.Topics.CDC, event.Table)
	}
	return kp.config.Topics.CDC
}

// validateKafkaConfig validates Kafka configuration
func validateKafkaConfig(cfg *config.KafkaConfig) error {
	if len(cfg.Brokers) == 0 {
		return ErrInvalidConfiguration
	}
	if cfg.Topics.CDC == "" {
		return ErrInvalidConfiguration
	}
	return nil
}

// parseAcks converts string acks configuration to kafka.RequiredAcks
func parseAcks(acks string) kafka.RequiredAcks {
	switch acks {
	case "all":
		return kafka.RequireAll
	case "1":
		return kafka.RequireOne
	case "0":
		return kafka.RequireNone
	default:
		return kafka.RequireAll // Default to all
	}
}

// parseCompression converts string compression to kafka.Compression
func parseCompression(compression string) kafka.Compression {
	switch compression {
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	case "none":
		return kafka.Gzip // Fall back to gzip for now
	default:
		return kafka.Gzip // Default to gzip
	}
}

// parseDuration parses duration string with fallback
func parseDuration(durationStr string) time.Duration {
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		return 10 * time.Millisecond // Default fallback
	}
	return duration
}
