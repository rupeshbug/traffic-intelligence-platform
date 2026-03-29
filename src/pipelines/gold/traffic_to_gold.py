import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    to_date,
    window,
    when
)

from src.utils.logger import logger
from src.utils.exception import TrafficPipelineException


def create_spark_session():
    logger.info("Creating Spark session for Silver to Gold pipeline")

    spark = (
        SparkSession.builder
        .appName("TrafficGoldLayer")
        .master("spark://spark-master:7077")
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


def read_silver_stream(spark):
    logger.info("Reading Silver stream from Delta location")

    silver_df = (
        spark.readStream
        .format("delta")
        .load("/opt/spark/warehouse/traffic_silver")
    )

    return silver_df


def build_dim_zone(silver_df):
    logger.info("Building dim_zone")

    dim_zone = (
        silver_df
        .select("city_zone")
        .dropDuplicates(["city_zone"])
        .withColumn(
            "zone_type",
            when(col("city_zone") == "CBD", "Commercial")
            .when(col("city_zone") == "TECHPARK", "IT Hub")
            .when(col("city_zone").isin("AIRPORT", "TRAINSTATION"), "Transit Hub")
            .otherwise("Residential")
        )
        .withColumn(
            "traffic_risk",
            when(col("city_zone").isin("CBD", "AIRPORT", "TRAINSTATION"), "HIGH")
            .when(col("city_zone") == "TECHPARK", "MEDIUM")
            .otherwise("LOW")
        )
    )

    return dim_zone


def build_dim_road(silver_df):
    logger.info("Building dim_road")

    dim_road = (
        silver_df
        .select("road_id")
        .dropDuplicates(["road_id"])
        .withColumn(
            "road_type",
            when(col("road_id").isin("R100", "R200"), "Highway")
            .otherwise("City Road")
        )
        .withColumn(
            "speed_limit",
            when(col("road_id").isin("R100", "R200"), 100)
            .otherwise(60)
        )
    )

    return dim_road


def build_dim_weather(silver_df):
    logger.info("Building dim_weather")

    dim_weather = (
        silver_df
        .select("weather")
        .dropDuplicates(["weather"])
        .withColumn(
            "weather_severity",
            when(col("weather") == "CLEAR", "LOW")
            .when(col("weather") == "RAIN", "MEDIUM")
            .when(col("weather").isin("FOG", "STORM"), "HIGH")
            .otherwise("UNKNOWN")
        )
    )

    return dim_weather


def build_fact_traffic(silver_df):
    logger.info("Building fact_traffic")

    fact_traffic = (
        silver_df
        .select(
            "vehicle_id",
            "road_id",
            "city_zone",
            "weather",
            "event_ts",
            "hour",
            "peak_flag",
            "speed_int",
            "speed_band",
            "traffic_volume_int",
            "traffic_band",
            "congestion_level_int",
            "incident_flag_int"
        )
        .withColumn("event_date", to_date("event_ts"))
        .withColumn("ingested_at", current_timestamp())
    )

    return fact_traffic


def build_zone_hourly_metrics(silver_df):
    logger.info("Building gold_zone_hourly_metrics")

    zone_hourly_metrics = (
        silver_df
        .withWatermark("event_ts", "15 minutes")
        .groupBy(
            window("event_ts", "1 hour"),
            "city_zone",
            "weather",
            "peak_flag"
        )
        .agg(
            count("*").alias("event_count"),
            avg("speed_int").alias("avg_speed"),
            avg("traffic_volume_int").alias("avg_traffic_volume"),
            avg("congestion_level_int").alias("avg_congestion_level"),
            spark_sum("incident_flag_int").alias("incident_count"),
            spark_min("speed_int").alias("min_speed"),
            spark_max("speed_int").alias("max_speed")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "city_zone",
            "weather",
            "peak_flag",
            "event_count",
            "avg_speed",
            "avg_traffic_volume",
            "avg_congestion_level",
            "incident_count",
            "min_speed",
            "max_speed"
        )
    )

    return zone_hourly_metrics


def build_road_hourly_metrics(silver_df):
    logger.info("Building gold_road_hourly_metrics")

    road_hourly_metrics = (
        silver_df
        .withWatermark("event_ts", "15 minutes")
        .groupBy(
            window("event_ts", "1 hour"),
            "road_id",
            "weather"
        )
        .agg(
            count("*").alias("event_count"),
            avg("speed_int").alias("avg_speed"),
            avg("traffic_volume_int").alias("avg_traffic_volume"),
            avg("congestion_level_int").alias("avg_congestion_level"),
            spark_sum("incident_flag_int").alias("incident_count")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "road_id",
            "weather",
            "event_count",
            "avg_speed",
            "avg_traffic_volume",
            "avg_congestion_level",
            "incident_count"
        )
    )

    return road_hourly_metrics


def write_stream(df, checkpoint_path, output_path, query_name):
    logger.info(f"Starting Gold write stream for {query_name}")

    query = (
        df.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .queryName(query_name)
        .start()
    )

    logger.info(f"Gold write stream started successfully for {query_name}")
    return query


def write_append_stream(df, checkpoint_path, output_path, query_name):
    logger.info(f"Starting Gold append stream for {query_name}")

    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .queryName(query_name)
        .start()
    )

    logger.info(f"Gold append stream started successfully for {query_name}")
    return query


def main():
    spark = None
    queries = []

    try:
        spark = create_spark_session()
        silver_df = read_silver_stream(spark)

        dim_zone = build_dim_zone(silver_df)
        dim_road = build_dim_road(silver_df)
        dim_weather = build_dim_weather(silver_df)
        fact_traffic = build_fact_traffic(silver_df)
        zone_hourly_metrics = build_zone_hourly_metrics(silver_df)
        road_hourly_metrics = build_road_hourly_metrics(silver_df)

        queries.append(
            write_append_stream(
                dim_zone,
                "/opt/spark/warehouse/chk/dim_zone",
                "/opt/spark/warehouse/dim_zone",
                "dim_zone"
            )
        )

        queries.append(
            write_append_stream(
                dim_road,
                "/opt/spark/warehouse/chk/dim_road",
                "/opt/spark/warehouse/dim_road",
                "dim_road"
            )
        )

        queries.append(
            write_append_stream(
                dim_weather,
                "/opt/spark/warehouse/chk/dim_weather",
                "/opt/spark/warehouse/dim_weather",
                "dim_weather"
            )
        )

        queries.append(
            write_append_stream(
                fact_traffic,
                "/opt/spark/warehouse/chk/fact_traffic",
                "/opt/spark/warehouse/fact_traffic",
                "fact_traffic"
            )
        )

        queries.append(
            write_stream(
                zone_hourly_metrics,
                "/opt/spark/warehouse/chk/gold_zone_hourly_metrics",
                "/opt/spark/warehouse/gold_zone_hourly_metrics",
                "gold_zone_hourly_metrics"
            )
        )

        queries.append(
            write_stream(
                road_hourly_metrics,
                "/opt/spark/warehouse/chk/gold_road_hourly_metrics",
                "/opt/spark/warehouse/gold_road_hourly_metrics",
                "gold_road_hourly_metrics"
            )
        )

        logger.info("Silver to Gold pipeline is now running")
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(str(TrafficPipelineException(e, sys)))
        raise TrafficPipelineException(e, sys) from e

    finally:
        for query in queries:
            if query is not None and query.isActive:
                query.stop()

        logger.info("Silver to Gold pipeline stopped")


if __name__ == "__main__":
    main()
    
