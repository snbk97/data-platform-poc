# DB-Stream Quick Start Guide

## 🚀 Quick Start

This guide will help you get DB-Stream up and running in minutes. Follow these steps to establish a working CDC pipeline with real-time data streaming from MySQL and PostgreSQL to ClickHouse.

## Prerequisites

### **System Requirements**
- **Docker**: 27.0 or later
- **Docker Compose**: 2.29 or later
- **Memory**: 16GB+ RAM recommended
- **Storage**: 100GB+ free disk space
- **Network**: Internet connection for image downloads

### **Platform Support**
- ✅ macOS (Intel and Apple Silicon)
- ✅ Linux (Ubuntu, CentOS, RHEL)
- ✅ Windows with WSL2
- ⚠️ Windows Native (experimental)

### **Optional Tools**
- **Git**: For cloning the repository
- **curl**: For API testing
- **Docker Desktop**: For GUI management

## 🏁 One-Command Setup

### **Step 1: Get the Code**
```bash
# Clone the repository
git clone https://github.com/your-org/db-stream.git
cd db-stream

# Or create from scratch if developing locally
mkdir -p db-stream && cd db-stream
```

### **Step 2: Environment Configuration**
```bash
# Copy environment template (if exists)
cp .env.example .env

# Verify environment variables
cat .env
```

### **Step 3: Start All Services**
```bash
# Start the complete stack
docker-compose up -d

# View startup progress
docker-compose logs -f
```

### **Step 4: Verify Installation**
```bash
# Check service status
docker-compose ps

# Wait for all services to be healthy (2-3 minutes)
docker-compose ps --format "table {{.Name}}\t{{.Status}}"
```

## 🌐 Access UI Tools

Once all services are running, access the UI tools:

| UI Tool | URL | Purpose |
|---------|-----|---------|
| **Kafka UI** | http://localhost:8080 | Monitor topics and messages |
| **Debezium UI** | http://localhost:8081 | Manage CDC connectors |
| **ClickHouse** | http://localhost:8123 | Query analytics database |

## ✅ Verify Data Flow

### **Test Database Connectivity**
```bash
# Test MySQL
docker-compose exec mysql mysql -u root -pdebezium -e "SHOW DATABASES;"

# Test PostgreSQL
docker-compose exec postgres psql -U postgres -c "\l"

# Test ClickHouse
docker-compose exec clickhouse clickhouse-client --query "SHOW TABLES;"
```

### **Test Kafka Connectivity**
```bash
# Check Kafka cluster
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# List topics
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Test Debezium API
curl http://localhost:8083/connectors
```

## 🔧 Setup CDC Connectors

### **Option 1: Use Debezium UI (Recommended)**
1. Open http://localhost:8081
2. Click "Create Connector"
3. Select "MySQL" or "PostgreSQL"
4. Fill in connection details
5. Click "Save"

### **Option 2: Use API**
```bash
# MySQL Connector
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
http://localhost:8083/connectors/ -d '{
  "name": "mysql-inventory-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "debezium",
    "database.server.id": "184054",
    "database.server.name": "dbstream-mysql",
    "database.include.list": "inventory",
    "database.history.kafka.bootstrap.servers": "kafka:29092",
    "database.history.kafka.topic": "schema-changes.inventory"
  }
}'

# PostgreSQL Connector
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
http://localhost:8083/connectors/ -d '{
  "name": "postgres-inventory-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "postgres",
    "database.server.name": "dbstream-postgres",
    "plugin.name": "pgoutput"
  }
}'
```

## 📊 Test End-to-End Pipeline

### **Generate Test Data**
```bash
# Add data to MySQL
docker-compose exec mysql mysql -u root -pdebezium inventory -e "
INSERT INTO customers (id, first_name, last_name, email) 
VALUES (1001, 'Test', 'Customer', 'test@example.com');"

# Add data to PostgreSQL
docker-compose exec postgres psql -U postgres -c "
INSERT INTO test_table (id, name, created_at) 
VALUES (1, 'Test Record', NOW());"
```

### **Verify CDC in Kafka**
```bash
# Check for new topics
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume CDC messages
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dbstream-mysql.inventory.customers \
  --from-beginning --max-messages 1
```

### **Verify Data in ClickHouse**
```bash
# Check ClickHouse tables
docker-compose exec clickhouse clickhouse-client --query "SHOW TABLES;"

# Query raw CDC data (after creating tables)
docker-compose exec clickhouse clickhouse-client --query "
SELECT * FROM mysql_raw_cdc 
ORDER BY timestamp DESC 
LIMIT 5 FORMAT Pretty;"
```

## 🛠️ Common Operations

### **Start/Stop Services**
```bash
# Stop all services
docker-compose down

# Stop and remove volumes (data loss)
docker-compose down -v

# Restart specific service
docker-compose restart debezium

# View logs
docker-compose logs -f kafka
```

### **Scaling Services**
```bash
# Scale Kafka brokers
docker-compose up -d --scale kafka=3

# Scale Debezium workers
docker-compose up -d --scale debezium=2
```

### **Managing Connectors**
```bash
# List all connectors
curl http://localhost:8083/connectors

# Get connector status
curl http://localhost:8083/connectors/mysql-inventory-connector/status

# Delete connector
curl -X DELETE http://localhost:8083/connectors/mysql-inventory-connector
```

## 🔍 Troubleshooting

### **Common Issues**

#### **Services Not Starting**
```bash
# Check logs for errors
docker-compose logs

# Check resource usage
docker stats

# Restart services
docker-compose down && docker-compose up -d
```

#### **Connector Failures**
```bash
# Check connector status and errors
curl http://localhost:8083/connectors/mysql-inventory-connector/status | jq

# Check connector configuration
curl http://localhost:8083/connectors/mysql-inventory-connector/config | jq

# Check Debezium logs
docker-compose logs debezium
```

#### **Database Connection Issues**
```bash
# Test MySQL connection
docker-compose exec mysql mysql -u root -pdebezium -e "SELECT 1;"

# Test PostgreSQL connection
docker-compose exec postgres psql -U postgres -c "SELECT 1;"

# Check database logs
docker-compose logs mysql
docker-compose logs postgres
```

#### **Kafka Issues**
```bash
# Check Kafka logs
docker-compose logs kafka

# Test Kafka broker
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check Zookeeper
docker-compose logs zookeeper
```

### **Performance Issues**

#### **High Memory Usage**
```bash
# Check memory usage
docker stats --no-stream

# Reduce memory limits in docker-compose.yml
# Look for: deploy.resources.limits.memory
```

#### **Slow CDC**
```bash
# Check connector lag
curl http://localhost:8083/connectors/mysql-inventory-connector/status | jq '.tasks[0].lag'

# Check Kafka metrics
docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group connect-mysql-inventory-connector
```

## 📚 Next Steps

### **Learn More**
- 📖 [Phase 1 Implementation Guide](04-phase1-implementation.md)
- 🏗️ [Architecture Overview](02-architecture.md)
- 🎯 [Technology Choices](06-technology-choices.md)
- 📊 [Visual Diagrams](03-diagrams.md)

### **Advanced Configuration**
- 🔧 [Docker Architecture](07-docker-architecture.md)
- 🚀 [Future Milestones](05-future-milestones.md)
- 📝 [Project Introduction](01-introduction.md)

### **Production Setup**
- 📊 Monitoring and alerting setup
- 🔒 Security configuration
- 💾 Backup and recovery procedures
- 📈 Performance optimization

## 🆘 Getting Help

### **Documentation**
- [Complete Documentation](../README.md)
- [Architecture Details](02-architecture.md)
- [FAQ Section](../docs/faq.md)

### **Community Support**
- 🐛 [GitHub Issues](https://github.com/your-org/db-stream/issues)
- 💬 [GitHub Discussions](https://github.com/your-org/db-stream/discussions)
- 📧 [Email Support](mailto:support@yourorg.com)

### **Quick Commands Reference**
```bash
# 🚀 Quick start
docker-compose up -d

# 📊 Check status
docker-compose ps

# 🔍 View logs
docker-compose logs -f

# 🛑 Stop all
docker-compose down

# 🔄 Restart service
docker-compose restart <service-name>

# 📈 Scale service
docker-compose up -d --scale <service-name>=<count>
```

---

**🎉 Congratulations!** You now have a working CDC pipeline. 

**Next Steps:** 
1. Explore the UI tools
2. Create sample applications
3. Set up monitoring
4. Plan production deployment

*Last Updated: 2026-02-10*
*Version: 1.0*