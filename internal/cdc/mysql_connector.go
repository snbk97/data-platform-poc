package cdc

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"db-stream/pkg/models"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

type MySQLConnector struct {
	name         string
	config       *MySQLConfig
	db           *sql.DB
	eventHandler EventHandler
	ddlHandler   DDLHandler
	logger       *zap.Logger
	status       *ConnectorStatus
	running      bool
	debeziumURL  string
}

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

type DDLHandler interface {
	HandleDDL(ctx context.Context, ddl *DDLEvent) error
}

type DDLEvent struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Database  string    `json:"database"`
	Table     string    `json:"table"`
	DDL       string    `json:"ddl"`
	Position  string    `json:"position"`
	Timestamp time.Time `json:"timestamp"`
	ServerID  int       `json:"server_id"`
}

type DebeziumConnectorConfig struct {
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
}

type DebeziumConnectorStatus struct {
	Name    string `json:"name"`
	State   string `json:"connector_state"`
	Worker  string `json:"worker_id"`
	Version string `json:"version"`
}

func NewMySQLConnector(name string, config *MySQLConfig, eventHandler EventHandler, ddlHandler DDLHandler, logger *zap.Logger) (*MySQLConnector, error) {
	if err := validateMySQLConfig(config); err != nil {
		return nil, fmt.Errorf("invalid mysql config: %w", err)
	}

	connector := &MySQLConnector{
		name:         name,
		config:       config,
		eventHandler: eventHandler,
		ddlHandler:   ddlHandler,
		logger:       logger,
		status: &ConnectorStatus{
			Name:        name,
			Type:        "mysql",
			State:       "STOPPED",
			EventsCount: 0,
			ErrorCount:  0,
			Metadata:    make(map[string]string),
		},
		running:     false,
		debeziumURL: "http://localhost:8083",
	}

	return connector, nil
}

func (mc *MySQLConnector) Start(ctx context.Context) error {
	mc.logger.Info("Starting MySQL CDC connector",
		zap.String("name", mc.name),
		zap.String("host", mc.config.Host),
		zap.String("database", mc.config.Database),
	)

	if err := mc.initDB(ctx); err != nil {
		mc.updateStatus("ERROR", err.Error())
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	if err := mc.setupDebeziumConnector(ctx); err != nil {
		mc.logger.Warn("Failed to setup Debezium connector, using native CDC", zap.Error(err))
	}

	mc.running = true
	mc.updateStatus("RUNNING", "")

	go mc.startCDCProcess(ctx)

	mc.logger.Info("MySQL CDC connector started successfully")
	return nil
}

func (mc *MySQLConnector) Stop(ctx context.Context) error {
	mc.logger.Info("Stopping MySQL CDC connector", zap.String("name", mc.name))

	mc.running = false

	if mc.db != nil {
		mc.db.Close()
		mc.db = nil
	}

	mc.updateStatus("STOPPED", "")

	mc.logger.Info("MySQL CDC connector stopped successfully")
	return nil
}

func (mc *MySQLConnector) GetName() string {
	return mc.name
}

func (mc *MySQLConnector) GetType() string {
	return "mysql"
}

func (mc *MySQLConnector) IsHealthy() bool {
	return mc.status.State == "RUNNING" && mc.db != nil
}

func (mc *MySQLConnector) GetStatus() *ConnectorStatus {
	status := *mc.status
	if mc.config != nil {
		status.Metadata["host"] = mc.config.Host
		status.Metadata["database"] = mc.config.Database
		status.Metadata["server_id"] = fmt.Sprintf("%d", mc.config.ServerID)
	}
	return &status
}

func (mc *MySQLConnector) SetDebeziumURL(url string) {
	mc.debeziumURL = url
}

func (mc *MySQLConnector) initDB(ctx context.Context) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true",
		mc.config.User,
		mc.config.Password,
		mc.config.Host,
		mc.config.Port,
		mc.config.Database,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	mc.db = db
	mc.logger.Info("Database connection established")
	return nil
}

func (mc *MySQLConnector) setupDebeziumConnector(ctx context.Context) error {
	connectorConfig := map[string]interface{}{
		"connector.class":                          "io.debezium.connector.mysql.MySqlConnector",
		"database.hostname":                        mc.config.Host,
		"database.port":                            mc.config.Port,
		"database.user":                            mc.config.User,
		"database.password":                        mc.config.Password,
		"database.server.id":                       mc.config.ServerID,
		"database.server.name":                     fmt.Sprintf("mysql-%s", mc.name),
		"database.include.list":                    mc.config.Database,
		"table.include.list":                       strings.Join(mc.config.IncludeTables, ","),
		"database.history.kafka.bootstrap.servers": "localhost:9092",
		"database.history.kafka.topic":             fmt.Sprintf("schema-changes.%s", mc.name),
		"include.schema.changes":                   "true",
		"transforms":                               "unwrap",
		"transforms.unwrap.type":                   "io.debezium.transforms.ExtractNewRecordState",
		"transforms.unwrap.drop.tombstones":        "false",
		"transforms.unwrap.delete.handling.mode":   "rewrite",
		"key.converter":                            "org.apache.kafka.connect.json.JsonConverter",
		"value.converter":                          "org.apache.kafka.connect.json.JsonConverter",
		"key.converter.schemas.enable":             "false",
		"value.converter.schemas.enable":           "false",
	}

	if mc.config.GTIDMode {
		connectorConfig["gtid.source.pattern"] = fmt.Sprintf("%s:.*", mc.config.Database)
		connectorConfig["use_gtid"] = "true"
	}

	switch mc.config.DDLHandlingMode {
	case "rewritten":
		connectorConfig["ddl.parser.mode"] = "rewritten"
	case "linked":
		connectorConfig["ddl.parser.mode"] = "linked"
	case "parse":
		connectorConfig["ddl.parser.mode"] = "parse"
	default:
		connectorConfig["ddl.parser.mode"] = "rewritten"
	}

	config := DebeziumConnectorConfig{
		Name:   fmt.Sprintf("mysql-%s", mc.name),
		Config: connectorConfig,
	}

	jsonData, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal connector config: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/connectors", mc.debeziumURL),
		bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create connector: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("failed to create connector: status %d", resp.StatusCode)
	}

	mc.logger.Info("Debezium connector created", zap.String("connector", fmt.Sprintf("mysql-%s", mc.name)))
	return nil
}

func (mc *MySQLConnector) startCDCProcess(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for mc.running {
		select {
		case <-ctx.Done():
			mc.logger.Info("CDC process stopped due to context cancellation")
			return
		case <-ticker.C:
			if err := mc.processChanges(ctx); err != nil {
				mc.logger.Error("Error processing changes", zap.Error(err))
				mc.status.ErrorCount++
				mc.updateStatus("ERROR", err.Error())
			}
		}
	}
}

func (mc *MySQLConnector) processChanges(ctx context.Context) error {
	for _, table := range mc.config.IncludeTables {
		if err := mc.processTableChanges(ctx, table); err != nil {
			return fmt.Errorf("failed to process changes for table %s: %w", table, err)
		}
	}

	if mc.config.DDLHandlingMode != "" {
		if err := mc.processDDLChanges(ctx); err != nil {
			mc.logger.Warn("Error processing DDL changes", zap.Error(err))
		}
	}

	return nil
}

func (mc *MySQLConnector) processTableChanges(ctx context.Context, table string) error {
	query := fmt.Sprintf(`
		SELECT id, '%s' as operation
		FROM %s
		WHERE updated_at > NOW() - INTERVAL '10 seconds'
		LIMIT 10`,
		table,
		mc.config.Database+"."+table,
	)

	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query table changes: %w", err)
	}
	defer rows.Close()

	var events []*models.CDCEvent
	for rows.Next() {
		var id int64

		if err := rows.Scan(&id); err != nil {
			mc.logger.Error("Failed to scan row", zap.Error(err))
			continue
		}

		event := &models.CDCEvent{
			ID:        mc.generateEventID(),
			Table:     table,
			Operation: "INSERT",
			Data:      fmt.Sprintf(`{"id": %d}`, id),
			Timestamp: time.Now().UTC(),
		}

		events = append(events, event)
	}

	if len(events) > 0 {
		if err := mc.eventHandler.HandleBatch(ctx, events); err != nil {
			return fmt.Errorf("failed to handle batch: %w", err)
		}

		mc.status.EventsCount += int64(len(events))
		mc.status.LastEvent = time.Now().UTC()
		mc.logger.Debug("Processed events",
			zap.String("table", table),
			zap.Int("count", len(events)),
		)
	}

	return nil
}

func (mc *MySQLConnector) processDDLChanges(ctx context.Context) error {
	query := `SHOW BINLOG EVENTS LIMIT 10`

	rows, err := mc.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query binlog events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var logName string
		var pos int
		var eventType string
		var serverID int
		var originalPos int
		var info string

		if err := rows.Scan(&logName, &pos, &eventType, &serverID, &originalPos, &info); err != nil {
			mc.logger.Error("Failed to scan binlog event", zap.Error(err))
			continue
		}

		if (strings.Contains(eventType, "Query") && strings.Contains(strings.ToLower(info), "create")) ||
			strings.Contains(strings.ToLower(info), "alter") ||
			strings.Contains(strings.ToLower(info), "drop") {

			ddlEvent := &DDLEvent{
				ID:        mc.generateDDLEventID(),
				Type:      "DDL",
				Database:  mc.config.Database,
				DDL:       info,
				Position:  fmt.Sprintf("%s:%d", logName, pos),
				Timestamp: time.Now().UTC(),
				ServerID:  serverID,
			}

			if mc.ddlHandler != nil {
				if err := mc.ddlHandler.HandleDDL(ctx, ddlEvent); err != nil {
					mc.logger.Error("Failed to handle DDL event", zap.Error(err))
				}
			}
		}
	}

	return nil
}

func (mc *MySQLConnector) generateEventID() string {
	return fmt.Sprintf("mysql-cdc-%s-%d", mc.name, time.Now().UnixNano())
}

func (mc *MySQLConnector) generateDDLEventID() string {
	return fmt.Sprintf("mysql-ddl-%s-%d", mc.name, time.Now().UnixNano())
}

func (mc *MySQLConnector) updateStatus(state, errorMessage string) {
	mc.status.State = state
	mc.status.ErrorMessage = errorMessage
	if state == "RUNNING" {
		mc.status.ErrorMessage = ""
	}
}

func validateMySQLConfig(config *MySQLConfig) error {
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.Port <= 0 || config.Port > 65535 {
		config.Port = 3306
	}
	if config.User == "" {
		return ErrInvalidConfiguration
	}
	if config.Database == "" {
		return ErrInvalidConfiguration
	}
	if config.ServerID == 0 {
		config.ServerID = 1
	}
	if config.GTIDMode {
		config.GTIDMode = true
	}
	if config.DDLHandlingMode == "" {
		config.DDLHandlingMode = "rewritten"
	}
	if len(config.IncludeTables) == 0 {
		config.IncludeTables = []string{}
	}
	return nil
}
