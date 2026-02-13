# Phase 1: Foundation Implementation Plan

## Overview

Phase 1 establishes the core infrastructure for the DB-Stream CDC pipeline. This phase focuses on setting up the fundamental services required for real-time Change Data Capture from relational databases to an analytical database.

## Phase 1 Objectives

### **Primary Goals**
1. ✅ **Establish Core Infrastructure**: Kafka, Zookeeper, Debezium Connect
2. ✅ **Setup CDC Pipeline**: MySQL and PostgreSQL to Kafka
3. ✅ **Configure Analytics Target**: ClickHouse for data storage
4. ✅ **Implement Monitoring**: UI tools for visibility and management
5. ✅ **Validate End-to-End Flow**: Prove the concept works

### **Success Criteria**
- CDC pipeline operational with sub-second latency
- Real-time data flow from source databases to ClickHouse
- UI tools successfully monitoring all components
- Schema validation and error handling working
- Documentation and deployment procedures complete

## Architecture Components

### **Core Services**

#### **Apache Kafka 3.7.0**
**Role**: Central event streaming backbone
**Configuration**: 
- 3 brokers (for HA in production, 1 for POC)
- Default topic replication factor: 1 (POC) / 3 (production)
- Retention: 7 days default
- Compression: lz4 for efficiency

**Key Topics**:
- `mysql-cdc.{database}.{table}` - MySQL change events
- `postgres-cdc.{database}.{table}` - PostgreSQL change events
- `connect-configs` - Debezium connector configurations
- `connect-offsets` - CDC offset tracking
- `connect-statuses` - Connector status monitoring

#### **Apache Zookeeper 3.9.2**
**Role**: Kafka cluster coordination
**Configuration**:
- Standalone mode for POC
- Integrated with Kafka configuration
- Data persistence via Docker volume

#### **Debezium Connect 3.0.1.Final**
**Role**: CDC connector management and execution
**Configuration**:
- Single worker instance (scale later)
- Key/value converters: JSON with Schema Registry
- Offset storage: Kafka topics
- Error handling: Dead letter queues

**Connector Types**:
- MySQL Connector (Binlog-based CDC)
- PostgreSQL Connector (Logical decoding)

#### **Source Databases**

##### **MySQL 8.4.3 LTS**
**Configuration**:
- Binlog enabled: `server-id=1`, `log-bin=mysql-bin`
- Row-based replication: `binlog_format=ROW`
- Character set: utf8mb4
- Storage engine: InnoDB

**Sample Data**:
- Sample e-commerce database
- Multiple tables with relationships
- Regular data generation for testing

##### **PostgreSQL 16.4**
**Configuration**:
- Logical decoding enabled: `wal_level=logical`
- Replication slots supported
- Standard PostgreSQL extensions

**Sample Data**:
- Sample CRM/database
- Mixed data types including JSON
- Referential integrity constraints

#### **ClickHouse 24.3.3**
**Role**: Analytics database for CDC data
**Configuration**:
- Kafka Engine for direct topic consumption
- MergeTree tables for analytics
- Time-based partitioning
- Compression codecs enabled

**Table Strategy**:
- Raw CDC tables (complete audit trail)
- Aggregated tables (pre-computed metrics)
- Materialized views (real-time aggregations)

### **UI Services**

#### **Kafka UI**
**Purpose**: Kafka cluster monitoring
**Features**:
- Topic browsing and message inspection
- Consumer group monitoring
- Broker health status
- Configuration management

#### **Debezium UI**
**Purpose**: Debezium connector management
**Features**:
- Connector creation wizard
- Status monitoring and alerts
- Configuration validation
- Error diagnosis tools

## Implementation Details

### **Docker Configuration**

#### **Network Architecture**
```yaml
networks:
  db-stream:
    driver: bridge
    internal: false  # Allow external access for UI tools
```

#### **Volume Strategy**
```yaml
volumes:
  kafka-data:
    driver: local
  mysql-data:
    driver: local
  postgres-data:
    driver: local
  clickhouse-data:
    driver: local
  zookeeper-data:
    driver: local
```

### **Service Dependencies**
```yaml
# Startup order
zookeeper → kafka → databases → debezium → ui-services
```

### **Environment Variables Management**

#### **Version Management (.env)**
```bash
# Kafka Stack
KAFKA_VERSION=3.7.0
ZOOKEEPER_VERSION=3.9.2
DEBEZIUM_VERSION=3.0.1.Final

# Databases
MYSQL_VERSION=8.4.3
POSTGRES_VERSION=16.4
CLICKHOUSE_VERSION=24.3.3

# UI Tools
KAFKA_UI_VERSION=latest
DEBEZIUM_UI_VERSION=latest
```

### **Connector Configuration Templates**

#### **MySQL Connector**
```json
{
  "name": "mysql-inventory-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.id": "184054",
    "database.server.name": "dbstream-mysql",
    "database.include.list": "inventory",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "schema-changes.inventory"
  }
}
```

#### **PostgreSQL Connector**
```json
{
  "name": "postgres-inventory-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.dbname": "postgres",
    "database.server.name": "dbstream-postgres",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot"
  }
}
```

### **ClickHouse Table Definitions**

#### **Raw MySQL CDC Table**
```sql
CREATE TABLE mysql_raw_cdc (
    timestamp DateTime,
    database String,
    table String,
    operation String,
    data String,
    metadata String
) ENGINE = Kafka('kafka:9092', 'mysql-cdc-inventory-', 'JSONEachRow')
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, database, table);
```

#### **Materialized View for Daily Metrics**
```sql
CREATE MATERIALIZED VIEW mysql_daily_metrics
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, database, table, operation)
AS SELECT
    toDate(timestamp) as date,
    database,
    table,
    operation,
    count() as record_count,
    avg(length(data)) as avg_data_size
FROM mysql_raw_cdc
GROUP BY date, database, table, operation;
```

## Step-by-Step Implementation

### **Step 1: Infrastructure Setup**
1. Create project directory structure
2. Set up version management (.env file)
3. Create Docker network and volumes
4. Configure Zookeeper and Kafka
5. Verify Kafka cluster functionality

### **Step 2: Database Configuration**
1. Deploy MySQL with binlog configuration
2. Deploy PostgreSQL with logical decoding
3. Create sample databases and data
4. Verify database connectivity and CDC readiness
5. Test binlog/logical decoding functionality

### **Step 3: Debezium Setup**
1. Deploy Debezium Connect service
2. Configure connector plugins
3. Register MySQL and PostgreSQL connectors
4. Verify connector status and topic creation
5. Test CDC data flow to Kafka

### **Step 4: ClickHouse Integration**
1. Deploy ClickHouse with Kafka Engine
2. Create tables for raw CDC data
3. Set up materialized views for analytics
4. Configure data retention policies
5. Verify data ingestion and query performance

### **Step 5: UI Tools Deployment**
1. Deploy Kafka UI for cluster monitoring
2. Deploy Debezium UI for connector management
3. Configure access credentials and security
4. Test UI functionality
5. Create monitoring dashboards

### **Step 6: End-to-End Testing**
1. Generate test data in source databases
2. Verify real-time CDC data flow
3. Validate data integrity in ClickHouse
4. Test error handling and recovery
5. Performance benchmarking

## Monitoring & Validation

### **Health Checks**
```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8083/connectors"]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 40s
```

### **Key Metrics to Monitor**
- **Connector Status**: Running, failed, paused states
- **Lag Metrics**: CDC pipeline delay
- **Throughput**: Records per second processed
- **Error Rates**: Failed transformations, connectivity issues
- **Resource Usage**: CPU, memory, disk I/O

### **Validation Scripts**
```bash
# Test MySQL CDC
mysql -h localhost -u root -p -e "INSERT INTO inventory.customers VALUES (1001, 'Test Customer', 'test@example.com');"

# Verify in Kafka
kafka-console-consumer --bootstrap-server localhost:9092 --topic dbstream-mysql.inventory.customers --from-beginning

# Check ClickHouse
clickhouse-client --query "SELECT * FROM mysql_raw_cdc WHERE database='inventory' AND table='customers' ORDER BY timestamp DESC LIMIT 1;"
```

## Troubleshooting Guide

### **Common Issues & Solutions**

#### **Debezium Connector Fails to Start**
**Symptoms**: Connector in FAILED state
**Causes**: Database connectivity, permissions, configuration errors
**Solutions**: 
- Check database connectivity and credentials
- Verify binlog/logical decoding configuration
- Review connector configuration syntax

#### **Kafka Topic Not Created**
**Symptoms**: No data in Kafka topics
**Causes**: Topic auto-creation disabled, permissions
**Solutions**:
- Enable topic auto-creation in Kafka
- Check Kafka broker logs for errors
- Verify topic naming conventions

#### **ClickHouse Data Ingestion Issues**
**Symptoms**: No data in ClickHouse tables
**Causes**: Kafka Engine configuration, format issues
**Solutions**:
- Verify Kafka Engine configuration
- Check JSON format compatibility
- Review ClickHouse error logs

#### **Performance Issues**
**Symptoms**: High latency, slow queries
**Causes**: Resource constraints, configuration tuning
**Solutions**:
- Monitor resource utilization
- Optimize Kafka and ClickHouse configurations
- Consider horizontal scaling

## Documentation & Knowledge Transfer

### **Deliverables**
1. **Docker Compose Configuration**: Complete, tested setup
2. **Connector Configuration**: Templates for MySQL and PostgreSQL
3. **ClickHouse Schema**: Table definitions and optimization
4. **Monitoring Setup**: Health checks and alerting
5. **Troubleshooting Guide**: Common issues and solutions
6. **API Documentation**: Debezium and Kafka endpoints

### **Training Materials**
1. **Architecture Overview**: System design and data flow
2. **Operational Procedures**: Start/stop, monitoring, maintenance
3. **Configuration Management**: Customization guidelines
4. **Performance Tuning**: Optimization strategies
5. **Security Best Practices**: Access control and encryption

---

*Last Updated: 2026-02-10*
*Document Version: 1.0*