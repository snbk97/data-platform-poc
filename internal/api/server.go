package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"db-stream/internal/config"
	"db-stream/pkg/models"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Server represents the HTTP API server
type Server struct {
	config         *config.Config
	logger         *zap.Logger
	router         *gin.Engine
	server         *http.Server
	clickhouseConn driver.Conn
}

// NewServer creates a new API server
func NewServer(cfg *config.Config, logger *zap.Logger) (*Server, error) {
	// Set Gin mode
	if cfg.Service.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	// Initialize router
	router := gin.New()
	router.Use(gin.Logger())
	router.Use(gin.Recovery())

	// Initialize ClickHouse connection
	chConn, err := initClickHouseConnection(&cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize ClickHouse: %w", err)
	}

	server := &Server{
		config:         cfg,
		logger:         logger,
		router:         router,
		clickhouseConn: chConn,
	}

	// Setup routes
	server.setupRoutes()

	return server, nil
}

// setupRoutes configures API routes
func (s *Server) setupRoutes() {
	// Health check
	s.router.GET("/health", s.healthCheck)

	// API v1 routes
	v1 := s.router.Group("/api/v1")
	{
		// Events endpoints
		v1.GET("/events", s.getEvents)
		v1.GET("/events/:id", s.getEvent)

		// Statistics endpoints
		v1.GET("/stats", s.getStats)
		v1.GET("/stats/users", s.getUserStats)
		v1.GET("/stats/products", s.getProductStats)
		v1.GET("/stats/orders", s.getOrderStats)
		v1.GET("/stats/daily", s.getDailyStats)

		// Metrics endpoints
		v1.GET("/metrics/throughput", s.getThroughputMetrics)
		v1.GET("/metrics/latency", s.getLatencyMetrics)
		v1.GET("/metrics/errors", s.getErrorMetrics)
	}
}

// Start begins serving the HTTP API
func (s *Server) Start(ctx context.Context) error {
	s.server = &http.Server{
		Addr:         ":" + s.config.Server.Port,
		Handler:      s.router,
		ReadTimeout:  parseDuration(s.config.Server.ReadTimeout),
		WriteTimeout: parseDuration(s.config.Server.WriteTimeout),
		IdleTimeout:  parseDuration(s.config.Server.IdleTimeout),
	}

	s.logger.Info("Starting API server", zap.String("port", s.config.Server.Port))

	return s.server.ListenAndServe()
}

// Shutdown gracefully stops the API server
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down API server")

	// Close ClickHouse connection
	if s.clickhouseConn != nil {
		s.clickhouseConn.Close()
	}

	return s.server.Shutdown(ctx)
}

// healthCheck returns the health status of the service
func (s *Server) healthCheck(c *gin.Context) {
	// Check ClickHouse connection
	chHealthy := s.checkClickHouseHealth()

	status := "unhealthy"
	if chHealthy {
		status = "healthy"
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  status,
		"service": s.config.Service.Name,
		"version": s.config.Service.Version,
		"checks": gin.H{
			"clickhouse": chHealthy,
		},
	})
}

// getEvents returns a paginated list of CDC events
func (s *Server) getEvents(c *gin.Context) {
	// Parse query parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	table := c.Query("table")
	operation := c.Query("operation")

	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 1000 {
		limit = 50
	}

	offset := (page - 1) * limit

	// Build query
	query := `
		SELECT id, table, operation, data, old_data, timestamp
		FROM cdc_events
		WHERE 1=1
	`
	args := []interface{}{}

	if table != "" {
		query += " AND table = ?"
		args = append(args, table)
	}

	if operation != "" {
		query += " AND operation = ?"
		args = append(args, operation)
	}

	query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	// Execute query
	rows, err := s.clickhouseConn.Query(c.Request.Context(), query, args...)
	if err != nil {
		s.logger.Error("Failed to query events", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query events"})
		return
	}
	defer rows.Close()

	var events []models.CDCEvent
	for rows.Next() {
		var event models.CDCEvent
		if err := rows.Scan(
			&event.ID,
			&event.Table,
			&event.Operation,
			&event.Data,
			&event.OldData,
			&event.Timestamp,
		); err != nil {
			s.logger.Error("Failed to scan event", zap.Error(err))
			continue
		}
		events = append(events, event)
	}

	// Get total count
	countQuery := "SELECT count() FROM cdc_events WHERE 1=1"
	countArgs := []interface{}{}
	if table != "" {
		countQuery += " AND table = ?"
		countArgs = append(countArgs, table)
	}
	if operation != "" {
		countQuery += " AND operation = ?"
		countArgs = append(countArgs, operation)
	}

	var total int64
	if err := s.clickhouseConn.QueryRow(c.Request.Context(), countQuery, countArgs...).Scan(&total); err != nil {
		s.logger.Error("Failed to get total count", zap.Error(err))
		total = int64(len(events))
	}

	c.JSON(http.StatusOK, gin.H{
		"events": events,
		"pagination": gin.H{
			"page":  page,
			"limit": limit,
			"total": total,
			"pages": (total + int64(limit) - 1) / int64(limit),
		},
	})
}

// getEvent returns a specific event by ID
func (s *Server) getEvent(c *gin.Context) {
	eventID := c.Param("id")

	query := `
		SELECT id, table, operation, data, old_data, timestamp
		FROM cdc_events
		WHERE id = ?
	`

	var event models.CDCEvent
	if err := s.clickhouseConn.QueryRow(c.Request.Context(), query, eventID).Scan(
		&event.ID,
		&event.Table,
		&event.Operation,
		&event.Data,
		&event.OldData,
		&event.Timestamp,
	); err != nil {
		s.logger.Error("Failed to query event", zap.String("id", eventID), zap.Error(err))
		c.JSON(http.StatusNotFound, gin.H{"error": "Event not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"event": event})
}

// getStats returns overall statistics
func (s *Server) getStats(c *gin.Context) {
	stats := make(map[string]interface{})

	// Total events
	var totalEvents int64
	if err := s.clickhouseConn.QueryRow(c.Request.Context(), "SELECT count() FROM cdc_events").Scan(&totalEvents); err != nil {
		s.logger.Error("Failed to get total events count", zap.Error(err))
		totalEvents = 0
	}
	stats["total_events"] = totalEvents

	// Events by operation
	type OpStats struct {
		Operation string `json:"operation"`
		Count     int64  `json:"count"`
	}
	var opStats []OpStats
	rows, _ := s.clickhouseConn.Query(c.Request.Context(),
		"SELECT operation, count() FROM cdc_events GROUP BY operation ORDER BY count DESC")
	for rows.Next() {
		var stat OpStats
		rows.Scan(&stat.Operation, &stat.Count)
		opStats = append(opStats, stat)
	}
	stats["by_operation"] = opStats

	// Events by table
	type TableStats struct {
		Table string `json:"table"`
		Count int64  `json:"count"`
	}
	var tableStats []TableStats
	rows, _ = s.clickhouseConn.Query(c.Request.Context(),
		"SELECT table, count() FROM cdc_events GROUP BY table ORDER BY count DESC LIMIT 10")
	for rows.Next() {
		var stat TableStats
		rows.Scan(&stat.Table, &stat.Count)
		tableStats = append(tableStats, stat)
	}
	stats["by_table"] = tableStats

	c.JSON(http.StatusOK, stats)
}

// getUserStats returns user-related statistics
func (s *Server) getUserStats(c *gin.Context) {
	query := `
		SELECT 
			COUNT() as total_users,
			countIf(operation = 'INSERT') as new_users,
			countIf(operation = 'UPDATE') as updated_users,
			countIf(operation = 'DELETE') as deleted_users
		FROM cdc_events 
		WHERE table = 'users'
		AND toDate(timestamp) >= toDate(now() - INTERVAL 30 DAY)
	`

	var totalUsers, newUsers, updatedUsers, deletedUsers int64
	err := s.clickhouseConn.QueryRow(c.Request.Context(), query).Scan(
		&totalUsers, &newUsers, &updatedUsers, &deletedUsers,
	)
	if err != nil {
		s.logger.Error("Failed to get user stats", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get user stats"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"total_users":   totalUsers,
		"new_users":     newUsers,
		"updated_users": updatedUsers,
		"deleted_users": deletedUsers,
		"period":        "30 days",
	})
}

// getProductStats returns product-related statistics
func (s *Server) getProductStats(c *gin.Context) {
	query := `
		SELECT 
			COUNT() as total_products,
			countIf(operation = 'INSERT') as new_products,
			countIf(operation = 'UPDATE') as updated_products,
			countIf(operation = 'DELETE') as deleted_products
		FROM cdc_events 
		WHERE table = 'products'
		AND toDate(timestamp) >= toDate(now() - INTERVAL 30 DAY)
	`

	var totalProducts, newProducts, updatedProducts, deletedProducts int64
	err := s.clickhouseConn.QueryRow(c.Request.Context(), query).Scan(
		&totalProducts, &newProducts, &updatedProducts, &deletedProducts,
	)
	if err != nil {
		s.logger.Error("Failed to get product stats", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get product stats"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"total_products":   totalProducts,
		"new_products":     newProducts,
		"updated_products": updatedProducts,
		"deleted_products": deletedProducts,
		"period":           "30 days",
	})
}

// getOrderStats returns order-related statistics
func (s *Server) getOrderStats(c *gin.Context) {
	query := `
		SELECT 
			COUNT() as total_orders,
			countIf(operation = 'INSERT') as new_orders,
			countIf(operation = 'UPDATE') as updated_orders,
			countIf(operation = 'DELETE') as deleted_orders
		FROM cdc_events 
		WHERE table = 'orders'
		AND toDate(timestamp) >= toDate(now() - INTERVAL 30 DAY)
	`

	var totalOrders, newOrders, updatedOrders, deletedOrders int64
	err := s.clickhouseConn.QueryRow(c.Request.Context(), query).Scan(
		&totalOrders, &newOrders, &updatedOrders, &deletedOrders,
	)
	if err != nil {
		s.logger.Error("Failed to get order stats", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get order stats"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"total_orders":   totalOrders,
		"new_orders":     newOrders,
		"updated_orders": updatedOrders,
		"deleted_orders": deletedOrders,
		"period":         "30 days",
	})
}

// getDailyStats returns daily statistics
func (s *Server) getDailyStats(c *gin.Context) {
	days, _ := strconv.Atoi(c.DefaultQuery("days", "7"))
	if days < 1 || days > 90 {
		days = 7
	}

	query := `
		SELECT 
			toDate(timestamp) as date,
			table,
			operation,
			count() as event_count
		FROM cdc_events 
		WHERE toDate(timestamp) >= toDate(now() - INTERVAL ? DAY)
		GROUP BY date, table, operation
		ORDER BY date DESC, event_count DESC
	`

	rows, err := s.clickhouseConn.Query(c.Request.Context(), query, days)
	if err != nil {
		s.logger.Error("Failed to get daily stats", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get daily stats"})
		return
	}
	defer rows.Close()

	type DailyStat struct {
		Date       time.Time `json:"date"`
		Table      string    `json:"table"`
		Operation  string    `json:"operation"`
		EventCount int64     `json:"event_count"`
	}

	var stats []DailyStat
	for rows.Next() {
		var stat DailyStat
		if err := rows.Scan(&stat.Date, &stat.Table, &stat.Operation, &stat.EventCount); err != nil {
			s.logger.Error("Failed to scan daily stat", zap.Error(err))
			continue
		}
		stats = append(stats, stat)
	}

	c.JSON(http.StatusOK, gin.H{
		"stats":  stats,
		"period": fmt.Sprintf("%d days", days),
	})
}

// getThroughputMetrics returns throughput metrics
func (s *Server) getThroughputMetrics(c *gin.Context) {
	hours, _ := strconv.Atoi(c.DefaultQuery("hours", "24"))
	if hours < 1 || hours > 168 {
		hours = 24
	}

	query := `
		SELECT 
			toStartOfHour(timestamp) as hour,
			count() as events_per_hour
		FROM cdc_events 
		WHERE timestamp >= now() - INTERVAL ? HOUR
		GROUP BY hour 
		ORDER BY hour DESC
	`

	rows, err := s.clickhouseConn.Query(c.Request.Context(), query, hours)
	if err != nil {
		s.logger.Error("Failed to get throughput metrics", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get throughput metrics"})
		return
	}
	defer rows.Close()

	type ThroughputMetric struct {
		Hour          time.Time `json:"hour"`
		EventsPerHour int64     `json:"events_per_hour"`
	}

	var metrics []ThroughputMetric
	for rows.Next() {
		var metric ThroughputMetric
		if err := rows.Scan(&metric.Hour, &metric.EventsPerHour); err != nil {
			s.logger.Error("Failed to scan throughput metric", zap.Error(err))
			continue
		}
		metrics = append(metrics, metric)
	}

	c.JSON(http.StatusOK, gin.H{
		"metrics": metrics,
		"period":  fmt.Sprintf("%d hours", hours),
	})
}

// getLatencyMetrics returns latency metrics
func (s *Server) getLatencyMetrics(c *gin.Context) {
	// For now, return simulated latency metrics
	// In a real implementation, you'd calculate actual processing latency
	c.JSON(http.StatusOK, gin.H{
		"metrics": []gin.H{
			{"source": "postgres", "avg_latency_ms": 150, "p95_latency_ms": 300, "p99_latency_ms": 500},
			{"source": "kafka", "avg_latency_ms": 10, "p95_latency_ms": 25, "p99_latency_ms": 50},
			{"source": "clickhouse", "avg_latency_ms": 50, "p95_latency_ms": 100, "p99_latency_ms": 200},
		},
	})
}

// getErrorMetrics returns error metrics
func (s *Server) getErrorMetrics(c *gin.Context) {
	// For now, return simulated error metrics
	// In a real implementation, you'd track actual errors
	c.JSON(http.StatusOK, gin.H{
		"metrics": []gin.H{
			{"component": "postgres-connector", "error_count": 0, "error_rate": 0.0},
			{"component": "kafka-producer", "error_count": 0, "error_rate": 0.0},
			{"component": "clickhouse-writer", "error_count": 0, "error_rate": 0.0},
		},
	})
}

// checkClickHouseHealth checks ClickHouse connection health
func (s *Server) checkClickHouseHealth() bool {
	if s.clickhouseConn == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.clickhouseConn.Ping(ctx); err != nil {
		s.logger.Error("ClickHouse health check failed", zap.Error(err))
		return false
	}

	return true
}

// initClickHouseConnection initializes ClickHouse connection for API
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
		DialTimeout:  30 * time.Second,
		MaxOpenConns: cfg.MaxOpenConns,
		MaxIdleConns: cfg.MaxIdleConns,
	}

	conn, err := clickhouse.Open(&options)
	if err != nil {
		return nil, err
	}

	// Test connection
	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}

	return conn, nil
}

// Helper function to parse duration strings
func parseDuration(durationStr string) time.Duration {
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		return 30 * time.Second // default fallback
	}
	return duration
}
