package models

import (
	"time"
)

// CDCEvent represents a change data capture event
type CDCEvent struct {
	ID        string    `json:"id" db:"id"`
	Table     string    `json:"table" db:"table_name"`
	Operation string    `json:"operation" db:"operation"` // INSERT, UPDATE, DELETE
	Data      string    `json:"data" db:"data"`           // JSON payload
	OldData   string    `json:"old_data" db:"old_data"`   // JSON payload (for UPDATE)
	Timestamp time.Time `json:"timestamp" db:"created_at"`
}

// ProcessedEvent represents an event that has been processed
type ProcessedEvent struct {
	ID         string    `json:"id"`
	CDCEventID string    `json:"cdc_event_id"`
	EventType  string    `json:"event_type"`
	Status     string    `json:"status"`
	Error      string    `json:"error,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

// User represents a user entity
type User struct {
	ID        int64     `json:"id" db:"id"`
	Email     string    `json:"email" db:"email"`
	FirstName string    `json:"first_name" db:"first_name"`
	LastName  string    `json:"last_name" db:"last_name"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// Product represents a product entity
type Product struct {
	ID          int64     `json:"id" db:"id"`
	Name        string    `json:"name" db:"name"`
	Description string    `json:"description" db:"description"`
	Price       float64   `json:"price" db:"price"`
	CategoryID  *int64    `json:"category_id" db:"category_id"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
}

// Order represents an order entity
type Order struct {
	ID          int64     `json:"id" db:"id"`
	UserID      int64     `json:"user_id" db:"user_id"`
	TotalAmount float64   `json:"total_amount" db:"total_amount"`
	Status      string    `json:"status" db:"status"`
	OrderDate   time.Time `json:"order_date" db:"order_date"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
}

// OrderItem represents an order item entity
type OrderItem struct {
	ID        int64   `json:"id" db:"id"`
	OrderID   int64   `json:"order_id" db:"order_id"`
	ProductID int64   `json:"product_id" db:"product_id"`
	Quantity  int     `json:"quantity" db:"quantity"`
	Price     float64 `json:"price" db:"price"`
}

// UserActivity represents user activity summary
type UserActivity struct {
	UserID       int64     `json:"user_id" db:"user_id"`
	Email        string    `json:"email" db:"email"`
	TotalEvents  uint64    `json:"total_events" db:"total_events"`
	LastActivity time.Time `json:"last_activity" db:"last_activity"`
}

// ProductStats represents product statistics
type ProductStats struct {
	ProductID   int64     `json:"product_id" db:"product_id"`
	Name        string    `json:"name" db:"name"`
	CategoryID  int64     `json:"category_id" db:"category_id"`
	TotalEvents uint64    `json:"total_events" db:"total_events"`
	AvgPrice    float64   `json:"avg_price" db:"avg_price"`
	LastUpdated time.Time `json:"last_updated" db:"last_updated"`
}

// OrderStats represents order statistics
type OrderStats struct {
	Date          time.Time `json:"date" db:"date"`
	TotalOrders   uint64    `json:"total_orders" db:"total_orders"`
	TotalAmount   float64   `json:"total_amount" db:"total_amount"`
	AvgOrderValue float64   `json:"avg_order_value" db:"avg_order_value"`
}

// HealthResponse represents health check response
type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
	Version string `json:"version"`
}

// ErrorResponse represents error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Code    int    `json:"code"`
}
