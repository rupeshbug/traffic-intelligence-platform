import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, desc

from src.utils.exception import TrafficPipelineException
from src.utils.logger import logger

FACT_TRAFFIC_PATH = "/opt/spark/warehouse/fact_traffic"
DIM_ZONE_PATH = "/opt/spark/warehouse/dim_zone"
DIM_ROAD_PATH = "/opt/spark/warehouse/dim_road"
DIM_WEATHER_PATH = "/opt/spark/warehouse/dim_weather"
ZONE_HOURLY_METRICS_PATH = "/opt/spark/warehouse/gold_zone_hourly_metrics"
ROAD_HOURLY_METRICS_PATH = "/opt/spark/warehouse/gold_road_hourly_metrics"


def create_spark_session():
    logger.info("Creating Spark session for Gold inspection")

    spark = (
        SparkSession.builder
        .appName("InspectTrafficGold")
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
    return spark


def load_delta_table(spark, path, table_name):
    logger.info(f"Loading Delta table: {table_name}")
    df = spark.read.format("delta").load(path)

    print(f"\n=== {table_name} COUNT ===")
    print(df.count())

    print(f"\n=== {table_name} SAMPLE ===")
    df.show(10, truncate=False)

    return df


def show_gold_validation_questions(fact_df, zone_metrics_df, road_metrics_df):
    print("\n=== AVG SPEED BY CONGESTION LEVEL ===")
    fact_df.groupBy("congestion_level_int").agg(
        avg("speed_int").alias("avg_speed"),
        count("*").alias("row_count")
    ).orderBy("congestion_level_int").show(truncate=False)

    print("\n=== AVG SPEED BY CITY ZONE ===")
    fact_df.groupBy("city_zone").agg(
        avg("speed_int").alias("avg_speed"),
        count("*").alias("row_count")
    ).orderBy(desc("row_count")).show(truncate=False)

    print("\n=== INCIDENT COUNT BY ZONE ===")
    fact_df.groupBy("city_zone").sum("incident_flag_int").withColumnRenamed(
        "sum(incident_flag_int)",
        "incident_count"
    ).orderBy(desc("incident_count")).show(truncate=False)

    print("\n=== TOP ZONE WINDOWS BY AVG CONGESTION ===")
    zone_metrics_df.select(
        "window_start",
        "window_end",
        "city_zone",
        "weather",
        "event_count",
        "avg_congestion_level",
        "avg_speed"
    ).orderBy(desc("avg_congestion_level"), desc("event_count")).show(10, truncate=False)

    print("\n=== TOP ROAD WINDOWS BY TRAFFIC VOLUME ===")
    road_metrics_df.select(
        "window_start",
        "window_end",
        "road_id",
        "weather",
        "event_count",
        "avg_traffic_volume",
        "avg_speed"
    ).orderBy(desc("avg_traffic_volume"), desc("event_count")).show(10, truncate=False)


def main():
    spark = None

    try:
        spark = create_spark_session()

        load_delta_table(spark, DIM_ZONE_PATH, "DIM_ZONE")
        load_delta_table(spark, DIM_ROAD_PATH, "DIM_ROAD")
        load_delta_table(spark, DIM_WEATHER_PATH, "DIM_WEATHER")
        fact_df = load_delta_table(spark, FACT_TRAFFIC_PATH, "FACT_TRAFFIC")
        zone_metrics_df = load_delta_table(
            spark,
            ZONE_HOURLY_METRICS_PATH,
            "GOLD_ZONE_HOURLY_METRICS"
        )
        road_metrics_df = load_delta_table(
            spark,
            ROAD_HOURLY_METRICS_PATH,
            "GOLD_ROAD_HOURLY_METRICS"
        )

        show_gold_validation_questions(fact_df, zone_metrics_df, road_metrics_df)

    except Exception as e:
        logger.error(str(TrafficPipelineException(e, sys)))
        raise TrafficPipelineException(e, sys) from e
    finally:
        if spark is not None:
            spark.stop()
        logger.info("Gold inspection finished")


if __name__ == "__main__":
    main()
