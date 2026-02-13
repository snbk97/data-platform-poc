package cdc

import (
	"errors"
	"fmt"
)

// Custom errors for CDC package
var (
	// ErrConnectorNotFound is returned when a connector is not found
	ErrConnectorNotFound = func(name string) error {
		return fmt.Errorf("connector '%s' not found", name)
	}

	// ErrConnectorExists is returned when a connector already exists
	ErrConnectorExists = func(name string) error {
		return fmt.Errorf("connector '%s' already exists", name)
	}

	// ErrConnectorNotStarted is returned when trying to operate on a stopped connector
	ErrConnectorNotStarted = errors.New("connector is not started")

	// ErrInvalidConfiguration is returned for invalid connector configuration
	ErrInvalidConfiguration = errors.New("invalid connector configuration")

	// ErrDatabaseConnection is returned for database connection failures
	ErrDatabaseConnection = errors.New("database connection failed")

	// ErrKafkaConnection is returned for Kafka connection failures
	ErrKafkaConnection = errors.New("kafka connection failed")

	// ErrEventProcessing is returned for event processing failures
	ErrEventProcessing = errors.New("event processing failed")

	// ErrUnsupportedOperation is returned for unsupported database operations
	ErrUnsupportedOperation = errors.New("unsupported database operation")
)
