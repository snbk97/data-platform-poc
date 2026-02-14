package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config represents the complete application configuration
type Config struct {
	Service    ServiceConfig    `yaml:"service"`
	Server     ServerConfig     `yaml:"server"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	Database   DatabaseConfig   `yaml:"database"`
	MySQL      MySQLConfig      `yaml:"mysql"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	Redis      RedisConfig      `yaml:"redis"`
	MinIO      MinIOConfig      `yaml:"minio"`
	Spark      SparkConfig      `yaml:"spark"`
	Modules    *ModulesConfig   `yaml:"modules"`
	Logging    LoggingConfig    `yaml:"logging"`
}

// ServiceConfig contains service-specific configuration
type ServiceConfig struct {
	Name        string `yaml:"name"`
	Version     string `yaml:"version"`
	Environment string `yaml:"environment"`
}

// ServerConfig contains HTTP server configuration
type ServerConfig struct {
	Port         string `yaml:"port"`
	ReadTimeout  string `yaml:"read_timeout"`
	WriteTimeout string `yaml:"write_timeout"`
	IdleTimeout  string `yaml:"idle_timeout"`
}

// KafkaConfig contains Kafka connection configuration
type KafkaConfig struct {
	Brokers         []string `yaml:"brokers"`
	GroupID         string   `yaml:"group_id"`
	Topics          Topics   `yaml:"topics"`
	BatchSize       int      `yaml:"batch_size"`
	FlushFrequency  string   `yaml:"flush_frequency"`
	CompressionType string   `yaml:"compression_type"`
	Acks            string   `yaml:"acks"`
}

// Topics contains Kafka topic configuration
type Topics struct {
	CDC        string `yaml:"cdc"`
	DDL        string `yaml:"ddl"`
	Processed  string `yaml:"processed"`
	DeadLetter string `yaml:"dead_letter"`
}

// DatabaseConfig contains database connection configuration
type DatabaseConfig struct {
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Database     string `yaml:"database"`
	SSLMode      string `yaml:"ssl_mode"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

// MySQLConfig contains MySQL connection configuration
type MySQLConfig struct {
	Host            string   `yaml:"host"`
	Port            int      `yaml:"port"`
	User            string   `yaml:"user"`
	Password        string   `yaml:"password"`
	Database        string   `yaml:"database"`
	ServerID        int      `yaml:"server_id"`
	GTIDMode        bool     `yaml:"gtid_mode"`
	DDLHandlingMode string   `yaml:"ddl_handling_mode"`
	IncludeSchemas  []string `yaml:"include_schemas"`
	IncludeTables   []string `yaml:"include_tables"`
}

// ClickHouseConfig contains ClickHouse connection configuration
type ClickHouseConfig struct {
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Database     string `yaml:"database"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

// RedisConfig contains Redis connection configuration
type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	Database int    `yaml:"database"`
}

// MinIOConfig contains MinIO connection configuration
type MinIOConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	UseSSL    bool   `yaml:"use_ssl"`
	Bucket    string `yaml:"bucket"`
}

// SparkConfig contains Spark connection configuration
type SparkConfig struct {
	MasterURL string   `yaml:"master_url"`
	AppName   string   `yaml:"app_name"`
	Jars      []string `yaml:"jars"`
}

// LoggingConfig contains logging configuration
type LoggingConfig struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	Output     string `yaml:"output"`
	MaxSize    int    `yaml:"max_size"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAge     int    `yaml:"max_age"`
}

// Load loads configuration from file and environment variables
func Load(serviceName string) (*Config, error) {
	v := viper.New()

	// Set config file name and paths
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("./configs")
	v.AddConfigPath("/app/configs")

	// Read config file
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Unmarshal config
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Override with environment variables
	overrideWithEnv(&cfg)

	// Set service-specific values
	cfg.Service.Name = serviceName
	if cfg.Service.Environment == "" {
		cfg.Service.Environment = "development"
	}

	return &cfg, nil
}

// overrideWithEnv overrides configuration with environment variables
func overrideWithEnv(cfg *Config) {
	// Server configuration
	if port := os.Getenv("SERVER_PORT"); port != "" {
		cfg.Server.Port = port
	}

	// Kafka configuration
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.Kafka.Brokers = strings.Split(brokers, ",")
	}
	if groupID := os.Getenv("KAFKA_GROUP_ID"); groupID != "" {
		cfg.Kafka.GroupID = groupID
	}

	// Database configuration
	if host := os.Getenv("DB_HOST"); host != "" {
		cfg.Database.Host = host
	}
	if user := os.Getenv("DB_USER"); user != "" {
		cfg.Database.User = user
	}
	if password := os.Getenv("DB_PASSWORD"); password != "" {
		cfg.Database.Password = password
	}
	if database := os.Getenv("DB_NAME"); database != "" {
		cfg.Database.Database = database
	}

	// ClickHouse configuration
	if chHost := os.Getenv("CLICKHOUSE_HOST"); chHost != "" {
		cfg.ClickHouse.Host = chHost
	}
	if chUser := os.Getenv("CLICKHOUSE_USER"); chUser != "" {
		cfg.ClickHouse.User = chUser
	}
	if chPassword := os.Getenv("CLICKHOUSE_PASSWORD"); chPassword != "" {
		cfg.ClickHouse.Password = chPassword
	}

	// Redis configuration
	if redisHost := os.Getenv("REDIS_HOST"); redisHost != "" {
		cfg.Redis.Host = redisHost
	}
	if redisPassword := os.Getenv("REDIS_PASSWORD"); redisPassword != "" {
		cfg.Redis.Password = redisPassword
	}

	// MinIO configuration
	if endpoint := os.Getenv("MINIO_ENDPOINT"); endpoint != "" {
		cfg.MinIO.Endpoint = endpoint
	}
	if accessKey := os.Getenv("MINIO_ACCESS_KEY"); accessKey != "" {
		cfg.MinIO.AccessKey = accessKey
	}
	if secretKey := os.Getenv("MINIO_SECRET_KEY"); secretKey != "" {
		cfg.MinIO.SecretKey = secretKey
	}

	// Logging configuration
	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		cfg.Logging.Level = logLevel
	}
}
