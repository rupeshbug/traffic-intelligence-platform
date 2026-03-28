The data arrives in stream that never ends.
The data processing is real-time.
Dashboard we create adapts to the data changes.

## Intro

This project simulates a real-world streaming data pipeline where traffic data is continuously generated, processed, cleaned, and transformed into analytics-ready datasets for business intelligence and dashboarding.

• Building a real-time streaming pipeline using Apache Kafka  
• Processing streaming data with PySpark Structured Streaming  
• Designing Bronze, Silver, Gold (Medallion Architecture)  
• Handling dirty and unreliable real-world data  
• Data cleaning, validation, and deduplication techniques  
• Working with Delta Lake for reliable and scalable storage  
• Implementing data quality checks in streaming pipelines  
• Feature engineering for analytics  
• Designing Star Schema (Fact & Dimension tables)  
• Creating analytics-ready datasets for BI tools  
• Connecting data pipelines to Power BI for dashboarding  
• Structuring production-style data engineering workflows  


## Tech Stack & Tools Used

Docker 
Apache Kafka  
Apache Spark / PySpark  
Delta Lake  
Hive Metastore  
Power B

## About data

This project uses a synthetic, real-time traffic event stream designed to closely mimic real-world urban traffic behavior. Each event represents a vehicle observation and includes attributes such as road ID, city zone, speed, congestion level, weather conditions, traffic volume, incident flags, and event timestamp. Unlike purely random data, the generator introduces realistic relationships between features—for example, higher congestion typically leads to lower speeds, adverse weather conditions reduce traffic flow efficiency, and rush-hour patterns vary by location such as CBDs, airports, and suburban areas. Additionally, the data stream intentionally includes anomalous and dirty records (e.g., null values, extreme speeds, schema drift, late or future timestamps, and corrupted payloads) to simulate real-world data quality challenges. This allows the pipeline to be tested not only for processing and analytics but also for robustness, validation, and cleaning in downstream stages.
