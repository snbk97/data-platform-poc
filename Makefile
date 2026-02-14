PHONY: help build dev test test-single test-coverage lint fmt up down logs clean dashboard

# Default target
help:
	@echo "Available commands:"
	@echo "  build         - Build all services"
	@echo "  dev           - Run with hot reload (development)"
	@echo "  test          - Run all tests"
	@echo "  test-single   - Run specific test file (usage: make test-single TEST=./path/to/test)"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  lint          - Run linter"
	@echo "  fmt           - Format code"
	@echo "  up            - Start all services with Docker Compose"
	@echo "  down          - Stop all services"
	@echo "  logs          - Show logs for all services"
	@echo "  clean         - Clean build artifacts and containers"
	@echo "  dashboard     - Open the web UI dashboard"

# Build targets
build:
	@echo "Building all services..."
	@mkdir -p bin
	go build -o bin/cdc ./cmd/cdc
	go build -o bin/api ./cmd/api
	go build -o bin/processor ./cmd/processor
	@echo "✓ All services built successfully"

# Development targets
dev:
	@echo "Starting development with hot reload..."
	@if command -v air >/dev/null 2>&1; then \
		air -c .air.toml; \
	else \
		echo "Installing air for hot reload..."; \
		go install github.com/air-verse/air@latest; \
		air -c .air.toml; \
	fi

# Test targets
test:
	@echo "Running all tests..."
	go test -v ./...

test-single:
	@if [ -z "$(TEST)" ]; then \
		echo "Usage: make test-single TEST=./path/to/test/file_test.go"; \
		exit 1; \
	fi
	@echo "Running test: $(TEST)"
	go test -v $(TEST)

test-coverage:
	@echo "Running tests with coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "✓ Coverage report generated: coverage.html"

# Code quality targets
lint:
	@echo "Running linter..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
		golangci-lint run; \
	fi

fmt:
	@echo "Formatting code..."
	go fmt ./...
	go mod tidy

# Docker targets
core-pg:
	@echo "Starting important services"
	docker-compose up -d kafka clickhouse zookeeper postgres debezium kafka-ui

core-mysql:
	# need to add mysql to docker file
	@echo "Starting important services"
	docker-compose up -d kafka clickhouse zookeeper mysql debezium kafka-ui

up:
	@echo "Starting all services..."
	docker-compose up -d

down:
	@echo "Stopping all services..."
	docker-compose down

logs:
	docker-compose logs -f

# Individual service targets
dev-cdc:
	@echo "Starting CDC service in development mode..."
	go run ./cmd/cdc

dev-api:
	@echo "Starting API service in development mode..."
	go run ./cmd/api

dev-processor:
	@echo "Starting Processor service in development mode..."
	go run ./cmd/processor

# Build individual services
build-cdc:
	@mkdir -p bin
	go build -o bin/cdc ./cmd/cdc

build-api:
	@mkdir -p bin
	go build -o bin/api ./cmd/api

build-processor:
	@mkdir -p bin
	go build -o bin/processor ./cmd/processor

# Clean targets
clean:
	@echo "Cleaning up..."
	rm -rf bin/
	rm -f coverage.out coverage.html
	docker-compose down -v --remove-orphans
	docker system prune -f

# Dashboard target
dashboard:
	@echo "Opening dashboard..."
	@if [ -f dashboard.html ]; then \
		open dashboard.html; \
	else \
		echo "Dashboard not found at dashboard.html"; \
	fi

# Installation targets
install-tools:
	@echo "Installing development tools..."
	go install github.com/air-verse/air@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Database targets
db-up:
	@echo "Starting database services..."
	docker-compose up -d kafka clickhouse postgres

db-down:
	@echo "Stopping database services..."
	docker-compose stop kafka clickhouse postgres

db-reset:
	@echo "Resetting databases..."
	docker-compose down -v
	docker-compose up -d kafka clickhouse postgres
	sleep 10
	@echo "✓ Databases reset successfully"

# Initialize project
init:
	@echo "Initializing DB-Stream project..."
	@if [ ! -f .env ]; then cp .env.example .env; echo "✓ Created .env from .env.example"; fi
	@if [ ! -f configs/config.yaml ]; then cp configs/config.example.yaml configs/config.yaml; echo "✓ Created config.yaml from example"; fi
	make install-tools
	go mod download
	@echo "✓ Project initialized successfully"

# Check if all required tools are installed
check-tools:
	@echo "Checking required tools..."
	@command -v go >/dev/null 2>&1 || (echo "❌ Go is not installed" && exit 1)
	@command -v docker >/dev/null 2>&1 || (echo "❌ Docker is not installed" && exit 1)
	@command -v docker-compose >/dev/null 2>&1 || (echo "❌ Docker Compose is not installed" && exit 1)
	@echo "✅ All required tools are installed"
