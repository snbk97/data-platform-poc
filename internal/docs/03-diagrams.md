# Architecture Diagrams & Visualizations

This document contains all Mermaid diagrams used throughout the DB-Stream project documentation. These diagrams provide visual representations of the architecture, data flow, and implementation details.

## Table of Contents

1. [Overall Data Architecture](#overall-data-architecture)
2. [Phase 1: Foundation Architecture](#phase-1-foundation-architecture)
3. [Complete POC Pipeline Flow](#complete-poc-pipeline-flow)
4. [Service Dependencies & Data Flow](#service-dependencies--data-flow)
5. [Docker Architecture Layout](#docker-architecture-layout)
6. [Development to Production Flow](#development-to-production-flow)

---

## Overall Data Architecture

```mermaid
graph TB
    subgraph "Data Sources"
        A[MySQL Database]
        B[PostgreSQL Database]
        C[S3 Files<br/>PDFs, CSVs, XLSX]
        D[API Data Sources]
    end

    subgraph "CDC Layer"
        E[Debezium Connect]
        F[Spark Processing]
        G[API Ingestion Service]
    end

    subgraph "Streaming Hub"
        H[Apache Kafka]
        I[Schema Registry]
    end

    subgraph "Processing Layer"
        J[Apache Flink<br/>Optional Stream Processing]
        K[Apache Airflow<br/>Orchestration]
        L[dbt<br/>SQL Transformations]
    end

    subgraph "Analytics Layer"
        M[ClickHouse<br/>Analytics Database]
        N[S3 Data Lake<br/>Raw Archive]
        O[Materialized Views<br/>Pre-computed Analytics]
    end

    subgraph "Consumers"
        P[Real-time Dashboard]
        Q[Bulk Analytics]
        R[Event-driven Services]
    end

    A --> E
    B --> E
    C --> F
    D --> G

    E --> H
    F --> H
    G --> H

    H --> I
    H --> J
    H --> M
    H --> N

    K --> F
    K --> L
    L --> M
    J --> O
    O --> P
    M --> Q
    H --> R

    style E fill:#ff9900,color:#fff
    style H fill:#231f20,color:#fff
    style M fill:#ffcc00,color:#000
```

**Key Insights:**
- **Centralized Streaming**: Kafka serves as the central hub for all data
- **Specialized Processing**: Different tools for different data types
- **Multiple Consumers**: Various downstream systems can consume the same data
- **Scalable Architecture**: Each layer can scale independently

---

## Phase 1: Foundation Architecture

```mermaid
graph TB
    subgraph "Core Services"
        K[Kafka Cluster<br/>3.7.0]
        Z[Zookeeper<br/>3.9.2]
        D[Debezium Connect<br/>3.0.1.Final]
    end

    subgraph "Data Sources"
        M[MySQL<br/>8.4.3 LTS]
        P[PostgreSQL<br/>16.4]
    end

    subgraph "Analytics Target"
        CH[ClickHouse<br/>24.3.3]
    end

    subgraph "UI Services"
        KUI[Kafka UI<br/>Web Interface]
        DUI[Debezium UI<br/>Connector Management]
        CHUI[ClickHouse Client<br/>Optional]
    end

    subgraph "Network"
        NW[db-stream Network<br/>All Services]
    end

    M -->|CDC Binlog| D
    P -->|Logical Decoding| D
    D -->|Change Events| K
    K -->|Raw CDC Data| CH

    Z --> K

    KUI --> K
    DUI --> D
    CHUI --> CH

    M -.-> NW
    P -.-> NW
    K -.-> NW
    D -.-> NW
    CH -.-> NW
    Z -.-> NW
    KUI -.-> NW
    DUI -.-> NW
    CHUI -.-> NW

    style D fill:#ff9900,color:#fff
    style K fill:#231f20,color:#fff
    style CH fill:#ffcc00,color:#000
```

**Phase 1 Focus:**
- **Core Infrastructure**: Kafka, Zookeeper, Debezium setup
- **CDC Pipeline**: MySQL and PostgreSQL to ClickHouse
- **Monitoring**: UI tools for visibility and management
- **Network Isolation**: All services on dedicated network

---

## Complete POC Pipeline Flow

```mermaid
flowchart LR
    subgraph "Phase 1: Foundation"
        direction TB
        P1A[RDBMS Data] --> P1B[Debezium CDC] --> P1C[Kafka] --> P1D[ClickHouse]
    end

    subgraph "Phase 2: File Processing"
        direction TB
        P2A[S3 Files] --> P2B[Spark Processing] --> P2C[Kafka] --> P2D[ClickHouse]
    end

    subgraph "Phase 3: API Integration"
        direction TB
        P3A[API Sources] --> P3B[Ingestion Service] --> P3C[Kafka] --> P3D[ClickHouse]
    end

    subgraph "Phase 4: Advanced Analytics"
        direction TB
        P4A[Airflow] --> P4B[dbt Transformations] --> P4C[Materialized Views] --> P4D[Analytics Dashboard]
    end

    P1D --> P4B
    P2D --> P4B
    P3D --> P4B
    P1C --> P4A
    P2C --> P4A
    P3C --> P4A
```

**Phased Development Approach:**
- **Incremental Build**: Each phase builds upon previous work
- **Parallel Streams**: Multiple data sources converge in Kafka
- **Unified Processing**: All data flows through common transformation layer
- **Analytics Layer**: Final destination with advanced capabilities

---

## Service Dependencies & Data Flow

```mermaid
graph TD
    subgraph "Infrastructure"
        ZK[Zookeeper]
        K[Kafka]
        SR[Schema Registry]
    end

    subgraph "Data Capture"
        DB[MySQL/PostgreSQL]
        DC[Debezium Connect]
    end

    subgraph "Processing & Storage"
        CH[ClickHouse]
        UI[UI Services]
    end

    subgraph "Data Topics"
        T1[mysql-cdc-topic]
        T2[postgres-cdc-topic]
        T3[analytics-topic]
    end

    ZK --> K
    K --> SR
    DB -->|Transaction Log| DC
    DC -->|Change Events| T1
    DC -->|Change Events| T2
    T1 --> K
    T2 --> K
    K -->|Stream| T3
    T3 --> CH
    SR -->|Schema Validation| DC
    SR -->|Schema Validation| CH

    UI -->|Monitor| K
    UI -->|Manage| DC
    UI -->|Query| CH

    style DC fill:#ff9900,color:#fff
    style K fill:#231f20,color:#fff
    style CH fill:#ffcc00,color:#000
```

**Service Interaction Details:**
- **Infrastructure First**: Zookeeper → Kafka → Schema Registry
- **Data Capture**: Database → Debezium → Kafka Topics
- **Schema Management**: Centralized validation and evolution
- **UI Integration**: Monitoring and management capabilities

---

## Docker Architecture Layout

```mermaid
graph TB
    subgraph "Host Machine"
        subgraph "Docker Engine"
            subgraph "db-stream Network"
                subgraph "Services"
                    K[Container: Kafka]
                    Z[Container: Zookeeper]
                    D[Container: Debezium]
                    M[Container: MySQL]
                    P[Container: PostgreSQL]
                    C[Container: ClickHouse]
                    UI1[Container: Kafka UI]
                    UI2[Container: Debezium UI]
                end
            end

            subgraph "Volumes"
                VK[volume: kafka-data]
                VDB[volume: mysql-data]
                VP[volume: postgres-data]
                VC[volume: clickhouse-data]
            end

            subgraph "Config Files"
                CF[docker-compose.yml]
                ENV[.env file]
                CONF[config/ directory]
            end
        end
    end

    K -.->|persists| VK
    M -.->|persists| VDB
    P -.->|persists| VP
    C -.->|persists| VC

    CF -->|configures| K
    CF -->|configures| D
    ENV -->|versions| CF
    CONF -->|custom| D
    CONF -->|custom| C

    style D fill:#ff9900,color:#fff
    style K fill:#231f20,color:#fff
    style C fill:#ffcc00,color:#000
```

**Container Organization:**
- **Isolated Network**: `db-stream` network for all containers
- **Persistent Storage**: Named volumes for data persistence
- **Configuration Management**: Centralized config files
- **Customization**: Dockerfiles for specialized configurations

---

## Development to Production Flow

```mermaid
journey
    title CDC POC Development Journey
    section Phase 1 - Foundation
      Setup Docker Environment: 5: Team
      Configure Debezium: 4: DevOps
      Test CDC Flow: 5: DataEngineer
      Verify ClickHouse Ingestion: 4: AnalyticsTeam
    section Phase 2 - File Processing
      Implement Spark Processing: 3: DataEngineer
      Handle Large PDF Files: 2: DevOps
      Optimize Performance: 3: Team
    section Phase 3 - API Integration
      Build Ingestion Service: 3: BackendTeam
      Add Error Handling: 4: DevOps
      Rate Limiting Setup: 3: Team
    section Phase 4 - Production
      Add Monitoring: 4: DevOps
      Security Hardening: 5: SecurityTeam
      Load Testing: 3: QATeam
      Documentation: 4: Team
```

**Development Timeline:**
- **Phase 1**: Foundation setup and basic CDC (Weeks 1-2)
- **Phase 2**: File processing capabilities (Weeks 3-4)
- **Phase 3**: API integration (Weeks 5-6)
- **Phase 4**: Production readiness (Weeks 7-8)

---

## Additional Reference Diagrams

### Kafka Topic Strategy
```mermaid
graph LR
    subgraph "Source Topics"
        T1[mysql.table1]
        T2[mysql.table2]
        T3[postgres.users]
        T4[postgres.orders]
    end
    
    subgraph "Processed Topics"
        P1[processed.customers]
        P2[processed.transactions]
    end
    
    subgraph "Analytics Topics"
        A1[analytics.daily_metrics]
        A2[analytics.real_time]
    end
    
    T1 --> P1
    T2 --> P2
    T3 --> P1
    T4 --> P2
    
    P1 --> A1
    P2 --> A2
```

### Data Flow Timeline
```mermaid
gantt
    title POC Implementation Timeline
    dateFormat  YYYY-MM-DD
    section Phase 1
    Docker Setup           :done, phase1a, 2026-02-10, 2d
    Kafka Configuration    :active, phase1b, 2026-02-12, 1d
    Debezium Setup         :phase1c, 2026-02-13, 2d
    ClickHouse Integration  :phase1d, 2026-02-15, 2d
    
    section Phase 2
    Spark Processing       :phase2a, 2026-02-17, 3d
    File Handling         :phase2b, 2026-02-20, 3d
    
    section Phase 3
    API Service           :phase3a, 2026-02-23, 3d
    Error Handling        :phase3b, 2026-02-26, 2d
    
    section Phase 4
    Monitoring           :phase4a, 2026-02-28, 3d
    Production Ready     :phase4b, 2026-03-03, 2d
```

---

## Diagram Usage Guidelines

### **When to Use Each Diagram:**

1. **Overall Data Architecture**: For high-level stakeholder presentations
2. **Phase 1 Foundation**: For development team implementation guidance
3. **Complete POC Pipeline**: For project planning and roadmap discussions
4. **Service Dependencies**: For technical architecture reviews
5. **Docker Layout**: For DevOps and infrastructure teams
6. **Development Journey**: For project management and team coordination

### **Color Coding:**
- **Orange** (`#ff9900`): Debezium/CDC components
- **Black** (`#231f20`): Kafka/streaming infrastructure
- **Yellow** (`#ffcc00`): Analytics and storage components

### **Customization Tips:**
- Update version numbers as components are upgraded
- Add new services as the architecture evolves
- Modify styling to match organization's brand guidelines
- Include performance metrics and KPIs where relevant

---

*Last Updated: 2026-02-10*
*Document Version: 1.0*