package config

import (
	"fmt"
)

// ModulesConfig contains configuration for enabling/disabling modules
type ModulesConfig struct {
	PostgreSQL *PostgreSQLModule `yaml:"postgresql"`
	MySQL      *MySQLModule      `yaml:"mysql"`
	MinIO      *MinIOModule      `yaml:"minio"`
	S3         *S3Module         `yaml:"s3"`
	API        *APIModule        `yaml:"api"`
	Processor  *ProcessorModule  `yaml:"processor"`
}

// PostgreSQLModule contains PostgreSQL-specific module configuration
type PostgreSQLModule struct {
	Enabled bool     `yaml:"enabled"`
	Tables  []string `yaml:"tables"`
}

// MySQLModule contains MySQL-specific module configuration
type MySQLModule struct {
	Enabled  bool     `yaml:"enabled"`
	Host     string   `yaml:"host"`
	Port     int      `yaml:"port"`
	User     string   `yaml:"user"`
	Password string   `yaml:"password"`
	Database string   `yaml:"database"`
	Tables   []string `yaml:"tables"`
}

// MinIOModule contains MinIO-specific module configuration
type MinIOModule struct {
	Enabled bool     `yaml:"enabled"`
	Buckets []string `yaml:"buckets"`
}

// S3Module contains S3-specific module configuration
type S3Module struct {
	Enabled bool     `yaml:"enabled"`
	Buckets []string `yaml:"buckets"`
	Region  string   `yaml:"region"`
}

// APIModule contains API-specific module configuration
type APIModule struct {
	Enabled       bool     `yaml:"enabled"`
	EnabledRoutes []string `yaml:"enabled_routes"`
}

// ProcessorModule contains Processor-specific module configuration
type ProcessorModule struct {
	Enabled       bool   `yaml:"enabled"`
	BatchSize     int    `yaml:"batch_size"`
	FlushInterval string `yaml:"flush_interval"`
}

// GetEnabledModules returns a list of enabled modules
func (c *Config) GetEnabledModules() []string {
	var enabledModules []string

	if c.Modules == nil {
		return enabledModules
	}

	if c.Modules.PostgreSQL != nil && c.Modules.PostgreSQL.Enabled {
		enabledModules = append(enabledModules, "postgresql")
	}

	if c.Modules.MySQL != nil && c.Modules.MySQL.Enabled {
		enabledModules = append(enabledModules, "mysql")
	}

	if c.Modules.MinIO != nil && c.Modules.MinIO.Enabled {
		enabledModules = append(enabledModules, "minio")
	}

	if c.Modules.S3 != nil && c.Modules.S3.Enabled {
		enabledModules = append(enabledModules, "s3")
	}

	if c.Modules.API != nil && c.Modules.API.Enabled {
		enabledModules = append(enabledModules, "api")
	}

	if c.Modules.Processor != nil && c.Modules.Processor.Enabled {
		enabledModules = append(enabledModules, "processor")
	}

	return enabledModules
}

// IsModuleEnabled checks if a specific module is enabled
func (c *Config) IsModuleEnabled(moduleName string) bool {
	if c.Modules == nil {
		return false
	}

	switch moduleName {
	case "postgresql":
		return c.Modules.PostgreSQL != nil && c.Modules.PostgreSQL.Enabled
	case "mysql":
		return c.Modules.MySQL != nil && c.Modules.MySQL.Enabled
	case "minio":
		return c.Modules.MinIO != nil && c.Modules.MinIO.Enabled
	case "s3":
		return c.Modules.S3 != nil && c.Modules.S3.Enabled
	case "api":
		return c.Modules.API != nil && c.Modules.API.Enabled
	case "processor":
		return c.Modules.Processor != nil && c.Modules.Processor.Enabled
	default:
		return false
	}
}

// ValidateModules validates module configuration
func (c *Config) ValidateModules() error {
	if c.Modules == nil {
		return nil
	}

	// Validate PostgreSQL module
	if c.Modules.PostgreSQL != nil && c.Modules.PostgreSQL.Enabled {
		if len(c.Modules.PostgreSQL.Tables) == 0 {
			return fmt.Errorf("postgresql module enabled but no tables specified")
		}
	}

	// Validate MySQL module
	if c.Modules.MySQL != nil && c.Modules.MySQL.Enabled {
		if len(c.Modules.MySQL.Tables) == 0 {
			return fmt.Errorf("mysql module enabled but no tables specified")
		}
	}

	// Validate MinIO module
	if c.Modules.MinIO != nil && c.Modules.MinIO.Enabled {
		if len(c.Modules.MinIO.Buckets) == 0 {
			return fmt.Errorf("minio module enabled but no buckets specified")
		}
	}

	// Validate S3 module
	if c.Modules.S3 != nil && c.Modules.S3.Enabled {
		if len(c.Modules.S3.Buckets) == 0 {
			return fmt.Errorf("s3 module enabled but no buckets specified")
		}
		if c.Modules.S3.Region == "" {
			return fmt.Errorf("s3 module enabled but no region specified")
		}
	}

	// Validate API module
	if c.Modules.API != nil && c.Modules.API.Enabled {
		// No specific validation needed for API
	}

	// Validate Processor module
	if c.Modules.Processor != nil && c.Modules.Processor.Enabled {
		if c.Modules.Processor.BatchSize <= 0 {
			return fmt.Errorf("processor module enabled but invalid batch size")
		}
		if c.Modules.Processor.FlushInterval == "" {
			return fmt.Errorf("processor module enabled but no flush interval specified")
		}
	}

	return nil
}
