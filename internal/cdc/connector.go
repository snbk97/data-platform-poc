package cdc

import (
	"context"
	"time"

	"db-stream/pkg/models"

	"go.uber.org/zap"
)

// Connector defines the interface for all CDC connectors
type Connector interface {
	// Start begins the CDC process for this connector
	Start(ctx context.Context) error

	// Stop gracefully stops the CDC process
	Stop(ctx context.Context) error

	// GetName returns the connector name
	GetName() string

	// GetType returns the connector type (postgresql, mysql, etc.)
	GetType() string

	// IsHealthy returns the health status of the connector
	IsHealthy() bool

	// GetStatus returns detailed status information
	GetStatus() *ConnectorStatus
}

// ConnectorStatus represents the status of a CDC connector
type ConnectorStatus struct {
	Name         string            `json:"name"`
	Type         string            `json:"type"`
	State        string            `json:"state"` // RUNNING, STOPPED, ERROR
	LastEvent    time.Time         `json:"last_event"`
	EventsCount  int64             `json:"events_count"`
	ErrorCount   int64             `json:"error_count"`
	ErrorMessage string            `json:"error_message,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// ConnectorConfig defines configuration for a CDC connector
type ConnectorConfig struct {
	Name     string                 `yaml:"name"`
	Type     string                 `yaml:"type"`
	Enabled  bool                   `yaml:"enabled"`
	Settings map[string]interface{} `yaml:"settings"`
}

// EventHandler defines the interface for handling CDC events
type EventHandler interface {
	// HandleEvent processes a CDC event
	HandleEvent(ctx context.Context, event *models.CDCEvent) error

	// HandleBatch processes a batch of CDC events
	HandleBatch(ctx context.Context, events []*models.CDCEvent) error
}

// ConnectorManager manages multiple CDC connectors
type ConnectorManager struct {
	connectors   map[string]Connector
	eventHandler EventHandler
	logger       *zap.Logger
}

// NewConnectorManager creates a new connector manager
func NewConnectorManager(eventHandler EventHandler, logger *zap.Logger) *ConnectorManager {
	return &ConnectorManager{
		connectors:   make(map[string]Connector),
		eventHandler: eventHandler,
		logger:       logger,
	}
}

// AddConnector adds a connector to the manager
func (cm *ConnectorManager) AddConnector(connector Connector) error {
	name := connector.GetName()
	if _, exists := cm.connectors[name]; exists {
		return ErrConnectorExists(name)
	}

	cm.connectors[name] = connector
	cm.logger.Info("Connector added", zap.String("name", name), zap.String("type", connector.GetType()))

	return nil
}

// RemoveConnector removes a connector from the manager
func (cm *ConnectorManager) RemoveConnector(name string) error {
	connector, exists := cm.connectors[name]
	if !exists {
		return ErrConnectorNotFound(name)
	}

	// Stop the connector before removing
	if err := connector.Stop(context.Background()); err != nil {
		cm.logger.Error("Failed to stop connector", zap.String("name", name), zap.Error(err))
	}

	delete(cm.connectors, name)
	cm.logger.Info("Connector removed", zap.String("name", name))

	return nil
}

// StartConnector starts a specific connector
func (cm *ConnectorManager) StartConnector(ctx context.Context, name string) error {
	connector, exists := cm.connectors[name]
	if !exists {
		return ErrConnectorNotFound(name)
	}

	return connector.Start(ctx)
}

// StopConnector stops a specific connector
func (cm *ConnectorManager) StopConnector(ctx context.Context, name string) error {
	connector, exists := cm.connectors[name]
	if !exists {
		return ErrConnectorNotFound(name)
	}

	return connector.Stop(ctx)
}

// StartAll starts all connectors
func (cm *ConnectorManager) StartAll(ctx context.Context) error {
	for name, connector := range cm.connectors {
		if err := connector.Start(ctx); err != nil {
			cm.logger.Error("Failed to start connector", zap.String("name", name), zap.Error(err))
			return err
		}
	}

	return nil
}

// StopAll stops all connectors
func (cm *ConnectorManager) StopAll(ctx context.Context) error {
	for name, connector := range cm.connectors {
		if err := connector.Stop(ctx); err != nil {
			cm.logger.Error("Failed to stop connector", zap.String("name", name), zap.Error(err))
		}
	}

	return nil
}

// GetConnector returns a specific connector
func (cm *ConnectorManager) GetConnector(name string) (Connector, bool) {
	connector, exists := cm.connectors[name]
	return connector, exists
}

// GetAllConnectors returns all connectors
func (cm *ConnectorManager) GetAllConnectors() map[string]Connector {
	return cm.connectors
}

// GetStatuses returns the status of all connectors
func (cm *ConnectorManager) GetStatuses() map[string]*ConnectorStatus {
	statuses := make(map[string]*ConnectorStatus)
	for name, connector := range cm.connectors {
		statuses[name] = connector.GetStatus()
	}
	return statuses
}
