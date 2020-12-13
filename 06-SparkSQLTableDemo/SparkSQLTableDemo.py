from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .enableHiveSupport() \
        .getOrCreate()


    logger = Log4j(spark)
    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # versao usando PARTITION
    flightTimeParquetDF.write \
        .mode("overwrite") \
        .partitionBy("ORIGIN", "OP_CARRIER") \
        .saveAsTable("flight_data_tbl")

    # versao usando BUCKET
    # flightTimeParquetDF.write \
    #     .format("csv") \
    #     .mode("overwrite") \
    #     .bucketBy(5, "ORIGIN", "OP_CARRIER") \
    #     .sortBy("OP_CARRIER", "ORIGIN") \
    #     .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))
