from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, year


def main():
    """
    Read raw crime CSV from HDFS, parse DATE OCC with time,
    add year column, and write cleaned data back to HDFS as CSV
    partitioned by year.
    """
    input_path = "hdfs://namenode:9000/datalake/raw/crime/crime_raw.csv"
    output_path = "hdfs://namenode:9000/datalake/curated/crime"
    spark = (
        SparkSession.builder
        .appName("CleanCrimeData")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )
    df = df.withColumn(
        "date_occ_ts",
        to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a")
    )
    df = df.withColumn("date_occ", to_date(col("date_occ_ts")))
    df = df.withColumn("year", year(col("date_occ")))
    df_clean = df.filter(col("date_occ").isNotNull())
    (
        df_clean
        .write
        .mode("overwrite")
        .option("header", "true")
        .partitionBy("year")
        .csv(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()
