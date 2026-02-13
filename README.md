# DB-Stream: Real-time CDC and Data Streaming Pipeline

A comprehensive Change Data Capture (CDC) and data streaming proof-of-concept built with Go, designed for real-time data ingestion and analytics.

## Quick Start

# Vscode / Cursor
- Install recommendation
- docse in [internal/docs](./internal/docs/)
- preview README.md / any markdown file (cmd+shift+V)

## Open Code
- Install [GSD](https://github.com/gsd-build/get-shit-done)

## Install Docker via Colima (Mac)
```bash
brew install colima docker docker-compose openssl
brew services restart colima

export CERTS=~/certs
export DOMAIN=registry-1.docker.io

[[ -d $CERTS ]] || mkdir $CERTS

echo | openssl s_client -showcerts -verify 5 -connect ${DOMAIN}:443 > ${CERTS}/${DOMAIN}.crt

colima ssh -- sudo cp ${CERTS}/* /usr/local/share/ca-certificates/
colima ssh -- sudo update-ca-certificates
colima restart
```

### Prerequisites
- Go 1.21+
- Docker & Docker Compose
- Make

# Build and run services
```bash
make build
make core-pg
```

Note: [PG CDC setup](./internal/docs/09-postgresql-cdc-guide.md)

### Development
```bash
# Run with hot reload
make dev

# Run tests
make test

# Run specific test
make test-single TEST=./internal/cdc/processor_test.go

# Lint code
make lint
```

## Architecture

The system consists of three main services:

1. **CDC Service** (`cmd/cdc`) - Captures database changes via Debezium
2. **API Service** (`cmd/api`) - REST API for querying and management
3. **Processor Service** (`cmd/processor`) - Processes events and stores in ClickHouse

## Project Structure
```
├── cmd/                  # Service entry points
│   ├── cdc/             # CDC service
│   ├── api/             # REST API service
│   └── processor/       # Event processor service
├── internal/             # Private application code
│   ├── config/          # Configuration management
│   ├── cdc/            # CDC logic
│   ├── api/            # API handlers and routes
│   ├── processor/      # Event processing logic
│   └── storage/        # Database abstractions
├── pkg/                 # Reusable packages
│   ├── logger/         # Structured logging
│   └── models/         # Data models
├── configs/            # Configuration files
├── scripts/            # Database scripts
└── docs/               # Documentation
```

## Configuration

The application uses YAML configuration files with environment variable overrides:

```bash
# Copy configuration
cp configs/config.example.yaml configs/config.yaml
cp .env.example .env

# Edit as needed
vim configs/config.yaml
vim .env
```

## Docker Stack

The full stack includes:
- **Kafka** - Event streaming
- **ClickHouse** - Analytics database
- **PostgreSQL** - Source database
- **Debezium** - CDC connector
- **Redis** - Caching
- **MinIO** - Object storage
- **Spark** - Big data processing
- **Grafana** - Monitoring

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

## Testing

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run specific test file
make test-single TEST=./path/to/test_test.go

# Run tests in a specific package
go test ./internal/cdc/...
```

## Build

```bash
# Build all services
make build

# Build individual services
make build-cdc
make build-api
make build-processor

# Cross-platform build
GOOS=linux GOARCH=amd64 make build
```

## Development Workflow

1. **Feature development**: Create a feature branch
2. **Make changes**: Use `make dev` for hot reload
3. **Test**: Run `make test` before committing
4. **Lint**: Run `make lint` and fix issues
5. **Integration test**: Use `make up` to spin up dependencies

## Monitoring

- **Grafana**: http://localhost:3000 (admin/admin)
- **Spark**: Temporarily disabled (see docker-compose.yml)
- **MinIO Console**: http://localhost:9002

## Documentation

Detailed documentation is available in `internal/docs/`:
- Architecture overview
- Implementation guides
- API documentation
- Deployment instructions

## Contributing

Follow the guidelines in `AGENTS.md` for code style and development practices.

## License

[License information]