import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

from src.utils.logger import logger
from src.utils.exception import TrafficPipelineException


def create_spark_session():
    logger.info("Creating Spark session for Kafka to Bronze pipeline")

    spark = (
        SparkSession.builder
        .appName("TrafficStreamingLakehouse")
        .master("spark://spark-master:7077")
        # delta lake package
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")

    return spark


def get_kafka_raw_stream(spark):
    logger.info("Initializing Kafka raw stream from topic: traffic-topic")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "traffic-topic")
        .option("startingOffsets", "latest")
        .load()
    )
    
    print("raw stream ------->", raw_stream)

    logger.info("Kafka raw stream initialized successfully")
    return raw_stream


def parse_stream(raw_stream):
    logger.info("Converting Kafka binary payload to string and applying schema")

    # deserialize the data -> convert binary to string
    json_stream = raw_stream.selectExpr(
        "CAST(value AS STRING) as raw_json",
        "timestamp as kafka_timestamp"
    )
    
    print("json stream ------->", json_stream)

    # Flexible Bronze schema (most of then casted to string type for now as they contain faulty data)
    traffic_schema = StructType([
        StructField("vehicle_id", StringType()),
        StructField("road_id", StringType()),
        StructField("city_zone", StringType()),
        StructField("speed", StringType()),
        StructField("congestion_level", IntegerType()),
        StructField("traffic_volume", IntegerType()),
        StructField("incident_flag", IntegerType()),
        StructField("weather", StringType()),
        StructField("event_time", StringType())
    ])

    parsed = json_stream.withColumn(
        "data",
        from_json(col("raw_json"), traffic_schema)
    )

    print("parsed -------> ", parsed)

    flattened = parsed.select(
        "raw_json",
        "kafka_timestamp",
        "data.*"
    )
    
    print("flattened -------> ", flattened)

    logger.info("Kafka stream parsed successfully into Bronze dataframe")
    return flattened


def write_bronze_stream(flattened):
    logger.info("Starting Bronze Delta write stream")

    bronze_query = (
        flattened.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_bronze")
        .option("path", "/opt/spark/warehouse/traffic_bronze")
        .start()
    )

    logger.info("Bronze Delta write stream started successfully")
    return bronze_query


def main():
    spark = None

    try:
        spark = create_spark_session()
        raw_stream = get_kafka_raw_stream(spark)
        flattened = parse_stream(raw_stream)
        write_bronze_stream(flattened)

        logger.info("Kafka to Bronze streaming pipeline is now running")
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(str(TrafficPipelineException(e, sys)))
        raise TrafficPipelineException(e, sys) from e

    finally:
        logger.info("Kafka to Bronze pipeline stopped")


if __name__ == "__main__":
    main()
    
