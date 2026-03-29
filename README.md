# 🚦 Real-Time Traffic Data Engineering Pipeline

## 📌 Overview

This project is an **end-to-end real-time data engineering pipeline** built using modern streaming technologies. It simulates realistic traffic data, processes it through a **multi-layered architecture**, and transforms it into **analytics-ready datasets**.

The goal is not just to build a pipeline, but to demonstrate:

- Strong data engineering fundamentals  
- Handling of messy, real-world data  
- System design thinking  
- Extensibility into analytics, ML, and monitoring  

---

## 🏗️ Architecture

Traffic Producer → Kafka → Spark → Bronze → Silver → Gold


### Tech Stack

- **Apache Kafka** → ingestion & buffering  
- **Apache Spark Structured Streaming** → stream processing  
- **Delta Lake** → storage layer  
- **Docker** → containerized environment  

---

## 🚦 Data Description

This project uses a **synthetic, real-time traffic event stream** designed to closely mimic real-world urban traffic behavior.

Each event represents a vehicle observation and includes attributes such as:

- vehicle ID  
- road ID  
- city zone  
- speed  
- congestion level  
- weather conditions  
- traffic volume  
- incident flags  
- event timestamp  

Unlike purely random data, the generator introduces **realistic relationships between features**:

- Higher congestion → lower speeds  
- Adverse weather → reduced traffic flow efficiency  
- Rush-hour patterns vary by location (CBD, airport, suburban areas)  

Additionally, the data stream intentionally includes **anomalous and dirty records**:

- null values  
- incorrect data types  
- extreme values (e.g., unrealistic speeds)  
- schema drift  
- late and future timestamps  
- corrupted payloads  

This allows the pipeline to be tested not only for processing and analytics, but also for:

- robustness  
- validation  
- data quality handling  

---

## 🥉 Bronze Layer — Raw Data

**Purpose:** Preserve raw truth

- Ingests data directly from Kafka  
- Minimal transformation  
- Flexible schema (permissive parsing)  
- Stores raw JSON for traceability  

> Bronze is a **source of truth**, not a place for cleaning.

---

## 🥈 Silver Layer — Clean & Validated Data

**Purpose:** Make data reliable and usable

### Key Operations

- Type casting (string → numeric, timestamp parsing)  
- Data quality validation (multiple checks per row)  
- Deduplication using event-time  
- Watermarking to handle late data  
- Feature engineering  

### Data Quality Design

Each record is evaluated using explicit checks:

- `parse_ok`  
- `required_fields_ok`  
- `speed_valid`  
- `traffic_volume_valid`  
- `incident_flag_valid`  
- `congestion_level_valid`  
- `weather_valid`  
- `time_valid`  

### 🚨 Key Design Decision

> Instead of silently dropping invalid rows, I preserved them in a separate rejected Delta stream with validation reasons, so the pipeline remains auditable and I can inspect upstream data quality issues without polluting downstream analytics.

### Output

- `traffic_silver` → clean, validated data  
- `traffic_silver_rejected` → rejected records with reasons  

---

## 🥇 Gold Layer — Analytics-Ready Data

**Purpose:** Serve business use cases

Gold transforms Silver data into **structured, meaningful datasets** optimized for analytics and dashboards.

---

### ⭐ Star Schema (Data Modeling)

The Gold layer uses a **star schema design**:

#### Fact Table

- `fact_traffic`  
- Contains event-level traffic observations:
  - speed  
  - congestion  
  - traffic volume  
  - time attributes  
  - contextual features  

#### Dimension Tables

- `dim_zone` → zone type, traffic risk  
- `dim_road` → road type, speed limits  
- `dim_weather` → weather severity  

These tables add **business meaning** and avoid repeated logic in analytical queries.

---

### 📊 Aggregate Tables (For Dashboards)

Gold also includes **precomputed hourly aggregate tables**:

- `gold_zone_hourly_metrics`  
- `gold_road_hourly_metrics`  

These tables compute metrics such as:

- average speed  
- traffic volume  
- congestion level  
- incident count  

Grouped by:

- time window (1 hour)  
- zone / road  
- weather  
- peak vs non-peak  

---

### ⏱️ Event-Time Processing

Aggregations use:

- **event-time windows** → `window(event_ts, "1 hour")`  
- **watermarking** to handle late-arriving data  

This ensures:

- correct time-based grouping  
- bounded state in streaming  

---

### Why Aggregates Matter

Instead of querying raw event data repeatedly:

- dashboards read precomputed metrics  
- queries become faster and simpler  
- system scales better  

> Fact tables store events, aggregate tables store insights.

---

## 🧠 Design Principles

- **Separation of concerns**
  - Kafka → ingestion  
  - Spark → processing  
  - Delta → storage  

- **Data quality first**
  - explicit validation  
  - explainable failures  
  - rejected data preserved  

- **Event-time correctness**
  - not relying on processing time  

- **Production-inspired design**
  - modular pipeline structure  
  - logging and exception handling  
  - containerized execution  

---

## 📂 Output Tables

### Silver

- `traffic_silver`  
- `traffic_silver_rejected`  

### Gold

- `fact_traffic`  
- `dim_zone`  
- `dim_road`  
- `dim_weather`  
- `gold_zone_hourly_metrics`  
- `gold_road_hourly_metrics`  

---

## 🚀 Future Work

This project will be extended with:

- 📊 Power BI dashboards  
- 🤖 Machine learning (traffic prediction)  
- 📉 data drift detection  
- 📡 monitoring and alerting  
- ⚙️ performance optimizations  

---
