import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    current_timestamp,
    expr,
    hour,
    lit,
    regexp_replace,
    rtrim,
    to_timestamp,
    when
)

from src.utils.logger import logger
from src.utils.exception import TrafficPipelineException

ALLOWED_WEATHER = ["CLEAR", "RAIN", "FOG", "STORM"]


def create_spark_session():
    logger.info("Creating Spark session for Bronze to Silver pipeline")

    spark = (
        SparkSession.builder
        .appName("TrafficSilverLayer")
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


def read_bronze_stream(spark):
    logger.info("Reading Bronze stream from Delta location")

    bronze_df = (
        spark.readStream
        .format("delta")
        .load("/opt/spark/warehouse/traffic_bronze")
    )

    return bronze_df


def add_data_quality_flags(bronze_df):
    logger.info("Applying Silver validation flags")

    dq_df = (
        bronze_df
        .withColumn("speed_int", col("speed").cast("int"))
        .withColumn("traffic_volume_int", col("traffic_volume").cast("int"))
        .withColumn("incident_flag_int", col("incident_flag").cast("int"))
        .withColumn("congestion_level_int", col("congestion_level").cast("int"))
        .withColumn("event_ts", to_timestamp("event_time"))
        .withColumn(
            "parse_ok",
            when(
                col("raw_json").isNotNull() &
                (
                    col("vehicle_id").isNotNull() |
                    col("road_id").isNotNull() |
                    col("city_zone").isNotNull() |
                    col("event_time").isNotNull()
                ),
                1
            ).otherwise(0)
        )
        .withColumn(
            "required_fields_ok",
            when(
                col("vehicle_id").isNotNull() &
                col("road_id").isNotNull() &
                col("city_zone").isNotNull() &
                col("event_time").isNotNull(),
                1
            ).otherwise(0)
        )
        .withColumn(
            "speed_valid",
            when((col("speed_int") >= 0) & (col("speed_int") <= 160), 1).otherwise(0)
        )
        .withColumn(
            "traffic_volume_valid",
            when((col("traffic_volume_int") >= 0) & (col("traffic_volume_int") <= 1000), 1).otherwise(0)
        )
        .withColumn(
            "incident_flag_valid",
            when(col("incident_flag_int").isin(0, 1), 1).otherwise(0)
        )
        .withColumn(
            "congestion_level_valid",
            when(col("congestion_level_int").between(1, 5), 1).otherwise(0)
        )
        .withColumn(
            "weather_valid",
            when(col("weather").isin(*ALLOWED_WEATHER), 1).otherwise(0)
        )
        .withColumn(
            "location_valid",
            when(
                col("road_id").isNotNull() &
                col("city_zone").isNotNull(),
                1
            ).otherwise(0)
        )
        .withColumn(
            "time_valid",
            when(
                col("event_ts").isNotNull() &
                (col("event_ts") <= current_timestamp() + expr("INTERVAL 10 MINUTES")),
                1
            ).otherwise(0)
        )
        .withColumn(
            "invalid_reason",
            rtrim(
                regexp_replace(
                    concat(
                        when(col("parse_ok") == 0, lit("PARSE_FAILURE|")).otherwise(lit("")),
                        when(col("required_fields_ok") == 0, lit("MISSING_REQUIRED_FIELDS|")).otherwise(lit("")),
                        when(col("speed_valid") == 0, lit("INVALID_SPEED|")).otherwise(lit("")),
                        when(col("traffic_volume_valid") == 0, lit("INVALID_TRAFFIC_VOLUME|")).otherwise(lit("")),
                        when(col("incident_flag_valid") == 0, lit("INVALID_INCIDENT_FLAG|")).otherwise(lit("")),
                        when(col("congestion_level_valid") == 0, lit("INVALID_CONGESTION_LEVEL|")).otherwise(lit("")),
                        when(col("weather_valid") == 0, lit("INVALID_WEATHER|")).otherwise(lit("")),
                        when(col("location_valid") == 0, lit("INVALID_LOCATION|")).otherwise(lit("")),
                        when(col("time_valid") == 0, lit("INVALID_EVENT_TIME|")).otherwise(lit(""))
                    ),
                    "\\|+",
                    "|"
                ),
                "|"
            )
        )
        .withColumn(
            "dq_flag",
            when(col("invalid_reason") == "", "OK").otherwise("REJECTED")
        )
    )

    return dq_df


def filter_good_records(validated_df):
    logger.info("Filtering valid Silver records")

    clean_stream = validated_df.filter(
        (col("dq_flag") == "OK") &
        (col("speed_valid") == 1) &
        (col("traffic_volume_valid") == 1) &
        (col("incident_flag_valid") == 1) &
        (col("congestion_level_valid") == 1) &
        (col("weather_valid") == 1) &
        (col("location_valid") == 1) &
        (col("time_valid") == 1)
    )

    return clean_stream


def capture_rejected_records(validated_df):
    logger.info("Capturing rejected Silver records")

    rejected_df = validated_df.filter(col("dq_flag") == "REJECTED")

    return rejected_df


def handle_late_data_and_deduplicate(clean_stream):
    logger.info("Applying watermarking and deduplication")

    watermarked_df = clean_stream.withWatermark("event_ts", "15 minutes")

    deduped_df = watermarked_df.dropDuplicates(["vehicle_id", "event_ts"])

    return deduped_df


def feature_engineering(deduped_df):
    logger.info("Applying Silver feature engineering")

    silver_final = (
        deduped_df
        .withColumn("hour", hour("event_ts"))
        .withColumn(
            "peak_flag",
            when(
                (col("hour").between(8, 11)) |
                (col("hour").between(17, 20)),
                1
            ).otherwise(0)
        )
        .withColumn(
            "speed_band",
            when(col("speed_int") < 30, "LOW")
            .when(col("speed_int") < 70, "MEDIUM")
            .otherwise("HIGH")
        )
        .withColumn(
            "traffic_band",
            when(col("traffic_volume_int") < 80, "LOW")
            .when(col("traffic_volume_int") < 140, "MEDIUM")
            .otherwise("HIGH")
        )
    )

    return silver_final


def write_silver_stream(silver_final):
    logger.info("Starting Silver Delta write stream")

    silver_query = (
        silver_final.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_silver")
        .option("path", "/opt/spark/warehouse/traffic_silver")
        .start()
    )

    logger.info("Silver Delta write stream started successfully")
    return silver_query


def write_rejected_stream(rejected_df):
    logger.info("Starting Silver rejected Delta write stream")

    rejected_query = (
        rejected_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_silver_rejected")
        .option("path", "/opt/spark/warehouse/traffic_silver_rejected")
        .start()
    )

    logger.info("Silver rejected Delta write stream started successfully")
    return rejected_query


def main():
    spark = None
    silver_query = None
    rejected_query = None

    try:
        spark = create_spark_session()
        bronze_df = read_bronze_stream(spark)
        validated_df = add_data_quality_flags(bronze_df)
        clean_stream = filter_good_records(validated_df)
        rejected_df = capture_rejected_records(validated_df)
        deduped_df = handle_late_data_and_deduplicate(clean_stream)
        silver_final = feature_engineering(deduped_df)
        silver_query = write_silver_stream(silver_final)
        rejected_query = write_rejected_stream(rejected_df)

        logger.info("Bronze to Silver pipeline is now running")
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(str(TrafficPipelineException(e, sys)))
        raise TrafficPipelineException(e, sys) from e

    finally:
        if silver_query is not None and silver_query.isActive:
            silver_query.stop()
        if rejected_query is not None and rejected_query.isActive:
            rejected_query.stop()
        logger.info("Bronze to Silver pipeline stopped")


if __name__ == "__main__":
    main()
    
