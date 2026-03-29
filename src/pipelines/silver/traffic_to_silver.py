import sys

from pyspark.sql import SparkSession

from src.utils.logger import logger
from src.utils.exception import TrafficPipelineException

# Spark Session Config
def create_spark_session():
    logger.info("Creating Spark session for Kafka to Bronze pipeline")

    spark = (
        SparkSession.builder
        .appName("TrafficSilverLayer")
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

# Read the data from bronze 
def read_bronze_stream(spark):
    logger.info("Reading Bronze stream from Delta location")
    bronze_df = spark.readStream.format("delta").load("/opt/spark/warehouse/traffic_bronze")
    bronze_df.printSchema()
    return bronze_df


def preview_silver_input(bronze_df):
    logger.info("Starting Silver preview stream in console")

    preview_query = (
        bronze_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(availableNow=True)
        .start()
    )

    return preview_query

def main():
    spark = None
    query = None

    try:
        spark = create_spark_session()
        bronze_df = read_bronze_stream(spark)
        query = preview_silver_input(bronze_df)

        logger.info("Bronze stream read successfully")
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
