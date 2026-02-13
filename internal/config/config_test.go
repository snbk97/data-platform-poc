package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigStructures(t *testing.T) {
	// Test that config structures can be created and have expected fields
	cfg := &Config{
		Service: ServiceConfig{
			Name:        "test-service",
			Version:     "1.0.0",
			Environment: "test",
		},
		Server: ServerConfig{
			Port: "8080",
		},
		Kafka: KafkaConfig{
			Brokers: []string{"localhost:9092"},
			GroupID: "test-group",
			Topics: Topics{
				CDC:        "test.cdc.events",
				Processed:  "test.processed.events",
				DeadLetter: "test.dead-letter.events",
			},
		},
	}

	// Basic assertions
	assert.Equal(t, "test-service", cfg.Service.Name)
	assert.Equal(t, "1.0.0", cfg.Service.Version)
	assert.Equal(t, "test", cfg.Service.Environment)
	assert.Equal(t, "8080", cfg.Server.Port)
	assert.Equal(t, []string{"localhost:9092"}, cfg.Kafka.Brokers)
	assert.Equal(t, "test-group", cfg.Kafka.GroupID)
	assert.Equal(t, "test.cdc.events", cfg.Kafka.Topics.CDC)
}

func TestOverrideWithEnv(t *testing.T) {
	// Test environment variable overrides
	cfg := &Config{
		Server: ServerConfig{
			Port: "8080",
		},
		Kafka: KafkaConfig{
			Brokers: []string{"localhost:9092"},
			GroupID: "default-group",
		},
		Database: DatabaseConfig{
			Host:     "localhost",
			Port:     5433,
			User:     "default",
			Password: "",
			Database: "default_db",
		},
	}

	// Set environment variables for testing
	os.Setenv("SERVER_PORT", "9090")
	os.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	os.Setenv("KAFKA_GROUP_ID", "env-group")
	os.Setenv("DB_HOST", "env-host")
	os.Setenv("DB_USER", "env-user")
	os.Setenv("DB_PASSWORD", "env-pass")
	os.Setenv("DB_NAME", "env-db")

	defer func() {
		os.Unsetenv("SERVER_PORT")
		os.Unsetenv("KAFKA_BROKERS")
		os.Unsetenv("KAFKA_GROUP_ID")
		os.Unsetenv("DB_HOST")
		os.Unsetenv("DB_USER")
		os.Unsetenv("DB_PASSWORD")
		os.Unsetenv("DB_NAME")
	}()

	// Apply environment overrides
	overrideWithEnv(cfg)

	// Assert that environment variables override config values
	assert.Equal(t, "9090", cfg.Server.Port)
	assert.Equal(t, []string{"broker1:9092", "broker2:9092"}, cfg.Kafka.Brokers)
	assert.Equal(t, "env-group", cfg.Kafka.GroupID)
	assert.Equal(t, "env-host", cfg.Database.Host)
	assert.Equal(t, "env-user", cfg.Database.User)
	assert.Equal(t, "env-pass", cfg.Database.Password)
	assert.Equal(t, "env-db", cfg.Database.Database)
}
