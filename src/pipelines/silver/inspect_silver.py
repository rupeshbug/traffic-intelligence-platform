import sys

from pyspark.sql import SparkSession

from src.utils.exception import TrafficPipelineException
from src.utils.logger import logger

SILVER_PATH = "/opt/spark/warehouse/traffic_silver"
REJECTED_PATH = "/opt/spark/warehouse/traffic_silver_rejected"


def create_spark_session():
    logger.info("Creating Spark session for Silver inspection")

    spark = (
        SparkSession.builder
        .appName("InspectTrafficSilver")
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


def inspect_delta_table(spark, path, table_name):
    logger.info(f"Inspecting Delta table: {table_name}")

    df = spark.read.format("delta").load(path)

    print(f"\n=== {table_name} SCHEMA ===")
    df.printSchema()

    print(f"\n=== {table_name} TOTAL COUNT ===")
    print(df.count())

    print(f"\n=== {table_name} SAMPLE ROWS ===")
    df.show(20, truncate=False)

    return df


def show_rejected_summary(rejected_df):
    print("\n=== REJECTED DATA SUMMARY ===")
    rejected_df.groupBy("invalid_reason").count().orderBy("count", ascending=False).show(
        20,
        truncate=False
    )


def main():
    spark = None

    try:
        spark = create_spark_session()
        inspect_delta_table(spark, SILVER_PATH, "TRAFFIC_SILVER")
        rejected_df = inspect_delta_table(spark, REJECTED_PATH, "TRAFFIC_SILVER_REJECTED")
        show_rejected_summary(rejected_df)

    except Exception as e:
        logger.error(str(TrafficPipelineException(e, sys)))
        raise TrafficPipelineException(e, sys) from e
    finally:
        if spark is not None:
            spark.stop()
        logger.info("Silver inspection finished")


if __name__ == "__main__":
    main()
