package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"db-stream/pkg/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// PostgresConnector implements CDC for PostgreSQL
type PostgresConnector struct {
	name         string
	config       *PostgresConfig
	db           *pgxpool.Pool
	eventHandler EventHandler
	logger       *zap.Logger
	status       *ConnectorStatus
	running      bool
}

// PostgresConfig contains PostgreSQL-specific configuration
type PostgresConfig struct {
	Host            string   `yaml:"host"`
	Port            int      `yaml:"port"`
	User            string   `yaml:"user"`
	Password        string   `yaml:"password"`
	Database        string   `yaml:"database"`
	SSLMode         string   `yaml:"ssl_mode"`
	Schema          string   `yaml:"schema"`
	ReplicationSlot string   `yaml:"replication_slot"`
	Publication     string   `yaml:"publication"`
	Tables          []string `yaml:"tables"`
}

// NewPostgresConnector creates a new PostgreSQL CDC connector
func NewPostgresConnector(name string, config *PostgresConfig, eventHandler EventHandler, logger *zap.Logger) (*PostgresConnector, error) {
	if err := validatePostgresConfig(config); err != nil {
		return nil, fmt.Errorf("invalid postgres config: %w", err)
	}

	connector := &PostgresConnector{
		name:         name,
		config:       config,
		eventHandler: eventHandler,
		logger:       logger,
		status: &ConnectorStatus{
			Name:        name,
			Type:        "postgresql",
			State:       "STOPPED",
			EventsCount: 0,
			ErrorCount:  0,
			Metadata:    make(map[string]string),
		},
		running: false,
	}

	return connector, nil
}

// Start begins the CDC process for PostgreSQL
func (pc *PostgresConnector) Start(ctx context.Context) error {
	pc.logger.Info("Starting PostgreSQL CDC connector",
		zap.String("name", pc.name),
		zap.String("host", pc.config.Host),
		zap.String("database", pc.config.Database),
	)

	// Initialize database connection
	if err := pc.initDB(ctx); err != nil {
		pc.updateStatus("ERROR", err.Error())
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	// Create replication slot if it doesn't exist
	if err := pc.createReplicationSlot(ctx); err != nil {
		pc.updateStatus("ERROR", err.Error())
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	// Create publication for tables
	if err := pc.createPublication(ctx); err != nil {
		pc.updateStatus("ERROR", err.Error())
		return fmt.Errorf("failed to create publication: %w", err)
	}

	pc.running = true
	pc.updateStatus("RUNNING", "")

	// Start CDC process
	go pc.startCDCProcess(ctx)

	pc.logger.Info("PostgreSQL CDC connector started successfully")
	return nil
}

// Stop gracefully stops the CDC process
func (pc *PostgresConnector) Stop(ctx context.Context) error {
	pc.logger.Info("Stopping PostgreSQL CDC connector", zap.String("name", pc.name))

	pc.running = false

	if pc.db != nil {
		pc.db.Close()
		pc.db = nil
	}

	pc.updateStatus("STOPPED", "")

	pc.logger.Info("PostgreSQL CDC connector stopped successfully")
	return nil
}

// GetName returns the connector name
func (pc *PostgresConnector) GetName() string {
	return pc.name
}

// GetType returns the connector type
func (pc *PostgresConnector) GetType() string {
	return "postgresql"
}

// IsHealthy returns the health status of the connector
func (pc *PostgresConnector) IsHealthy() bool {
	return pc.status.State == "RUNNING" && pc.db != nil
}

// GetStatus returns detailed status information
func (pc *PostgresConnector) GetStatus() *ConnectorStatus {
	// Copy status to avoid concurrent access issues
	status := *pc.status
	if pc.config != nil {
		status.Metadata["host"] = pc.config.Host
		status.Metadata["database"] = pc.config.Database
		status.Metadata["replication_slot"] = pc.config.ReplicationSlot
		status.Metadata["publication"] = pc.config.Publication
	}
	return &status
}

// initDB initializes the database connection
func (pc *PostgresConnector) initDB(ctx context.Context) error {
	connString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		pc.config.Host,
		pc.config.Port,
		pc.config.User,
		pc.config.Password,
		pc.config.Database,
		pc.config.SSLMode,
	)

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return fmt.Errorf("failed to parse connection config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	pc.db = pool
	pc.logger.Info("Database connection established")
	return nil
}

// createReplicationSlot creates a logical replication slot if it doesn't exist
func (pc *PostgresConnector) createReplicationSlot(ctx context.Context) error {
	query := fmt.Sprintf(`
		SELECT FROM pg_replication_slots 
		WHERE slot_name = '%s' 
		AND plugin = 'pgoutput'`,
		pc.config.ReplicationSlot,
	)

	var exists bool
	err := pc.db.QueryRow(ctx, query).Scan(&exists)
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to check replication slot: %w", err)
	}

	if !exists {
		createSlotQuery := fmt.Sprintf(
			"SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
			pc.config.ReplicationSlot,
		)

		_, err := pc.db.Exec(ctx, createSlotQuery)
		if err != nil {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}

		pc.logger.Info("Replication slot created", zap.String("slot", pc.config.ReplicationSlot))
	}

	return nil
}

// createPublication creates a publication for the specified tables
func (pc *PostgresConnector) createPublication(ctx context.Context) error {
	// Check if publication exists
	checkPubQuery := fmt.Sprintf(`
		SELECT FROM pg_publication 
		WHERE pubname = '%s'`,
		pc.config.Publication,
	)

	var exists bool
	err := pc.db.QueryRow(ctx, checkPubQuery).Scan(&exists)
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to check publication: %w", err)
	}

	if !exists {
		// Create publication for all tables in schema
		createPubQuery := fmt.Sprintf(
			"CREATE PUBLICATION %s FOR TABLE %s",
			pc.config.Publication,
			pc.getTablesList(),
		)

		_, err := pc.db.Exec(ctx, createPubQuery)
		if err != nil {
			return fmt.Errorf("failed to create publication: %w", err)
		}

		pc.logger.Info("Publication created", zap.String("publication", pc.config.Publication))
	}

	return nil
}

// getTablesList returns a comma-separated list of tables for the publication
func (pc *PostgresConnector) getTablesList() string {
	if len(pc.config.Tables) == 0 {
		return fmt.Sprintf("ALL IN SCHEMA %s", pc.config.Schema)
	}

	var tables []string
	for _, table := range pc.config.Tables {
		tables = append(tables, fmt.Sprintf("%s.%s", pc.config.Schema, table))
	}

	return strings.Join(tables, ", ")
}

// startCDCProcess starts the main CDC process
func (pc *PostgresConnector) startCDCProcess(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for pc.running {
		select {
		case <-ctx.Done():
			pc.logger.Info("CDC process stopped due to context cancellation")
			return
		case <-ticker.C:
			if err := pc.processChanges(ctx); err != nil {
				pc.logger.Error("Error processing changes", zap.Error(err))
				pc.status.ErrorCount++
				pc.updateStatus("ERROR", err.Error())
			}
		}
	}
}

// processChanges simulates processing database changes
// In a real implementation, this would use logical replication
func (pc *PostgresConnector) processChanges(ctx context.Context) error {
	// For this implementation, we'll simulate CDC by querying recent changes
	// In production, you would use the PostgreSQL logical replication protocol

	for _, table := range pc.config.Tables {
		if err := pc.processTableChanges(ctx, table); err != nil {
			return fmt.Errorf("failed to process changes for table %s: %w", table, err)
		}
	}

	return nil
}

// processTableChanges processes changes for a specific table
func (pc *PostgresConnector) processTableChanges(ctx context.Context, table string) error {
	// Simple simulation - in production, use logical replication
	query := fmt.Sprintf(`
		SELECT 'INSERT' as operation, 
		       row_to_json(t) as data
		FROM %s.%s t 
		WHERE updated_at > NOW() - INTERVAL '10 seconds'
		LIMIT 10`,
		pc.config.Schema, table,
	)

	rows, err := pc.db.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query table changes: %w", err)
	}
	defer rows.Close()

	var events []*models.CDCEvent
	for rows.Next() {
		var operation string
		var data json.RawMessage

		if err := rows.Scan(&operation, &data); err != nil {
			pc.logger.Error("Failed to scan row", zap.Error(err))
			continue
		}

		event := &models.CDCEvent{
			ID:        pc.generateEventID(),
			Table:     table,
			Operation: operation,
			Data:      string(data),
			Timestamp: time.Now().UTC(),
		}

		events = append(events, event)
	}

	if len(events) > 0 {
		if err := pc.eventHandler.HandleBatch(ctx, events); err != nil {
			return fmt.Errorf("failed to handle batch: %w", err)
		}

		pc.status.EventsCount += int64(len(events))
		pc.status.LastEvent = time.Now().UTC()
		pc.logger.Debug("Processed events",
			zap.String("table", table),
			zap.Int("count", len(events)),
		)
	}

	return nil
}

// generateEventID generates a unique event ID
func (pc *PostgresConnector) generateEventID() string {
	return fmt.Sprintf("pg-cdc-%s-%d", pc.name, time.Now().UnixNano())
}

// updateStatus updates the connector status
func (pc *PostgresConnector) updateStatus(state, errorMessage string) {
	pc.status.State = state
	pc.status.ErrorMessage = errorMessage
	if state == "RUNNING" {
		pc.status.ErrorMessage = ""
	}
}

// validatePostgresConfig validates PostgreSQL configuration
func validatePostgresConfig(config *PostgresConfig) error {
	if config.Host == "" {
		return ErrInvalidConfiguration
	}
	if config.Port <= 0 || config.Port > 65535 {
		return ErrInvalidConfiguration
	}
	if config.User == "" {
		return ErrInvalidConfiguration
	}
	if config.Database == "" {
		return ErrInvalidConfiguration
	}
	if config.ReplicationSlot == "" {
		config.ReplicationSlot = "debezium_slot"
	}
	if config.Publication == "" {
		config.Publication = "debezium_pub"
	}
	if config.Schema == "" {
		config.Schema = "public"
	}
	return nil
}
