package cdc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"db-stream/internal/config"

	"go.uber.org/zap"
)

// Processor handles CDC events from the source database
type Processor struct {
	config           *config.Config
	connectorManager *ConnectorManager
	kafkaProducer    *KafkaProducer
	logger           *zap.Logger
	running          bool
	mu               sync.RWMutex
	connectors       map[string]*ConnectorConfig
}

// NewProcessor creates a new CDC processor
func NewProcessor(cfg *config.Config, logger *zap.Logger) (*Processor, error) {
	// Create Kafka producer
	kafkaProducer, err := NewKafkaProducer(&cfg.Kafka, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Create connector manager
	connectorManager := NewConnectorManager(kafkaProducer, logger)

	processor := &Processor{
		config:           cfg,
		connectorManager: connectorManager,
		kafkaProducer:    kafkaProducer,
		logger:           logger,
		running:          false,
		connectors:       make(map[string]*ConnectorConfig),
	}

	// Load connector configurations
	if err := processor.loadConnectorConfigs(); err != nil {
		return nil, fmt.Errorf("failed to load connector configs: %w", err)
	}

	return processor, nil
}

// Start begins processing CDC events
func (p *Processor) Start(ctx context.Context) error {
	p.logger.Info("Starting CDC processor")

	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("CDC processor is already running")
	}
	p.running = true
	p.mu.Unlock()

	// Initialize and start enabled connectors
	if err := p.initializeConnectors(ctx); err != nil {
		return fmt.Errorf("failed to initialize connectors: %w", err)
	}

	// Start connector manager
	if err := p.connectorManager.StartAll(ctx); err != nil {
		return fmt.Errorf("failed to start connectors: %w", err)
	}

	// Start health check goroutine
	go p.healthCheckLoop(ctx)

	p.logger.Info("CDC processor started successfully")
	return nil
}

// Shutdown gracefully stops the CDC processor
func (p *Processor) Shutdown(ctx context.Context) error {
	p.logger.Info("Shutting down CDC processor")

	p.mu.Lock()
	p.running = false
	p.mu.Unlock()

	// Stop all connectors
	if err := p.connectorManager.StopAll(ctx); err != nil {
		p.logger.Error("Error stopping connectors", zap.Error(err))
	}

	// Close Kafka producer
	if err := p.kafkaProducer.Close(); err != nil {
		p.logger.Error("Error closing Kafka producer", zap.Error(err))
	}

	p.logger.Info("CDC processor shutdown completed")
	return nil
}

// AddConnector dynamically adds a new connector
func (p *Processor) AddConnector(ctx context.Context, config *ConnectorConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	connector, err := p.createConnector(config)
	if err != nil {
		return fmt.Errorf("failed to create connector: %w", err)
	}

	if err := p.connectorManager.AddConnector(connector); err != nil {
		return fmt.Errorf("failed to add connector to manager: %w", err)
	}

	p.connectors[config.Name] = config
	p.logger.Info("Connector added successfully",
		zap.String("name", config.Name),
		zap.String("type", config.Type))

	return nil
}

// RemoveConnector dynamically removes a connector
func (p *Processor) RemoveConnector(ctx context.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.connectorManager.RemoveConnector(name); err != nil {
		return fmt.Errorf("failed to remove connector: %w", err)
	}

	delete(p.connectors, name)
	p.logger.Info("Connector removed successfully", zap.String("name", name))

	return nil
}

// GetConnectorStatus returns status of all connectors
func (p *Processor) GetConnectorStatus() map[string]*ConnectorStatus {
	return p.connectorManager.GetStatuses()
}

// IsRunning returns whether the processor is running
func (p *Processor) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running
}

// loadConnectorConfigs loads connector configurations from config
func (p *Processor) loadConnectorConfigs() error {
	// For now, create a default PostgreSQL connector if not configured
	if len(p.connectors) == 0 {
		p.connectors["postgres-source"] = &ConnectorConfig{
			Name:    "postgres-source",
			Type:    "postgresql",
			Enabled: true,
			Settings: map[string]interface{}{
				"host":             "localhost",
				"port":             5433,
				"user":             "db_stream",
				"password":         "db_stream_password",
				"database":         "db_stream_source",
				"ssl_mode":         "disable",
				"schema":           "public",
				"replication_slot": "debezium_slot",
				"publication":      "debezium_pub",
				"tables":           []string{"users", "products", "orders"},
			},
		}
	}
	return nil
}

// initializeConnectors initializes all enabled connectors
func (p *Processor) initializeConnectors(ctx context.Context) error {
	for name, config := range p.connectors {
		if !config.Enabled {
			p.logger.Info("Connector disabled, skipping", zap.String("name", name))
			continue
		}

		connector, err := p.createConnector(config)
		if err != nil {
			return fmt.Errorf("failed to create connector %s: %w", name, err)
		}

		if err := p.connectorManager.AddConnector(connector); err != nil {
			return fmt.Errorf("failed to add connector %s: %w", name, err)
		}

		p.logger.Info("Connector initialized", zap.String("name", name), zap.String("type", config.Type))
	}

	return nil
}

// createConnector creates a connector based on type
func (p *Processor) createConnector(config *ConnectorConfig) (Connector, error) {
	switch config.Type {
	case "postgresql":
		return p.createPostgresConnector(config)
	case "mysql":
		return p.createMySQLConnector(config)
	default:
		return nil, fmt.Errorf("unsupported connector type: %s", config.Type)
	}
}

// createPostgresConnector creates a PostgreSQL connector
func (p *Processor) createPostgresConnector(config *ConnectorConfig) (Connector, error) {
	postgresConfig := &PostgresConfig{
		Host:            getStringSetting(config.Settings, "host", "localhost"),
		Port:            getIntSetting(config.Settings, "port", 5432),
		User:            getStringSetting(config.Settings, "user", "postgres"),
		Password:        getStringSetting(config.Settings, "password", ""),
		Database:        getStringSetting(config.Settings, "database", "postgres"),
		SSLMode:         getStringSetting(config.Settings, "ssl_mode", "disable"),
		Schema:          getStringSetting(config.Settings, "schema", "public"),
		ReplicationSlot: getStringSetting(config.Settings, "replication_slot", "debezium_slot"),
		Publication:     getStringSetting(config.Settings, "publication", "debezium_pub"),
		Tables:          getStringSliceSetting(config.Settings, "tables"),
	}

	return NewPostgresConnector(config.Name, postgresConfig, p.kafkaProducer, p.logger)
}

// createMySQLConnector creates a MySQL connector (placeholder)
func (p *Processor) createMySQLConnector(config *ConnectorConfig) (Connector, error) {
	// TODO: Implement MySQL connector
	return nil, fmt.Errorf("MySQL connector not yet implemented")
}

// healthCheckLoop runs periodic health checks
func (p *Processor) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.performHealthCheck()
		}
	}
}

// performHealthCheck performs health check on all components
func (p *Processor) performHealthCheck() {
	if !p.IsRunning() {
		return
	}

	statuses := p.connectorManager.GetStatuses()
	for name, status := range statuses {
		if status.State == "ERROR" {
			p.logger.Error("Connector in error state",
				zap.String("name", name),
				zap.String("error", status.ErrorMessage))
		}
	}
}

// Helper functions for config extraction
func getStringSetting(settings map[string]interface{}, key, defaultValue string) string {
	if val, ok := settings[key].(string); ok && val != "" {
		return val
	}
	return defaultValue
}

func getIntSetting(settings map[string]interface{}, key string, defaultValue int) int {
	if val, ok := settings[key].(int); ok {
		return val
	}
	if val, ok := settings[key].(float64); ok {
		return int(val)
	}
	return defaultValue
}

func getStringSliceSetting(settings map[string]interface{}, key string) []string {
	if val, ok := settings[key].([]string); ok {
		return val
	}
	if val, ok := settings[key].([]interface{}); ok {
		result := make([]string, len(val))
		for i, v := range val {
			if s, ok := v.(string); ok {
				result[i] = s
			}
		}
		return result
	}
	return []string{}
}
