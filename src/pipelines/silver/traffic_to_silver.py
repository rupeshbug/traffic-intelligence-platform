import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    to_timestamp,
    current_timestamp,
    expr,
    hour
)

from src.utils.logger import logger
from src.utils.exception import TrafficPipelineException


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
    logger.info("Applying data quality flags")

    dq_df = (
        bronze_df.withColumn(
            "dq_flag",
            when(col("vehicle_id").isNull(), "MISSING_VEHICLE")
            .when(col("event_time").isNull(), "MISSING_TIME")
            .when(col("raw_json").isNull(), "MISSING_RAW_JSON")
            .when(col("raw_json").contains("broken-payload"), "CORRUPT_JSON")
            .otherwise("OK")
        )
    )

    return dq_df


def safe_type_cast(dq_df):
    logger.info("Applying safe type casting")

    typed_df = (
        dq_df.withColumn("speed_int", col("speed").cast("int"))
        .withColumn("traffic_volume_int", col("traffic_volume").cast("int"))
        .withColumn("incident_flag_int", col("incident_flag").cast("int"))
        .withColumn("event_ts", to_timestamp("event_time"))
    )

    return typed_df


def apply_business_validations(typed_df):
    logger.info("Applying business validation rules")

    validated_df = (
        typed_df.withColumn(
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
            "time_valid",
            when(
                col("event_ts") <= current_timestamp() + expr("INTERVAL 10 MINUTES"),
                1
            ).otherwise(0)
        )
    )

    return validated_df


def filter_good_records(validated_df):
    logger.info("Filtering valid Silver records")

    clean_stream = validated_df.filter(
        (col("dq_flag") == "OK") &
        (col("speed_valid") == 1) &
        (col("traffic_volume_valid") == 1) &
        (col("incident_flag_valid") == 1) &
        (col("time_valid") == 1)
    )

    return clean_stream


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


def main():
    spark = None
    query = None

    try:
        spark = create_spark_session()
        bronze_df = read_bronze_stream(spark)
        dq_df = add_data_quality_flags(bronze_df)
        typed_df = safe_type_cast(dq_df)
        validated_df = apply_business_validations(typed_df)
        clean_stream = filter_good_records(validated_df)
        deduped_df = handle_late_data_and_deduplicate(clean_stream)
        silver_final = feature_engineering(deduped_df)
        query = write_silver_stream(silver_final)

        logger.info("Bronze to Silver pipeline is now running")
        query.awaitTermination()

    except Exception as e:
        logger.error(str(TrafficPipelineException(e, sys)))
        raise TrafficPipelineException(e, sys) from e

    finally:
        if query is not None and query.isActive:
            query.stop()
        logger.info("Bronze to Silver pipeline stopped")


if __name__ == "__main__":
    main()
    