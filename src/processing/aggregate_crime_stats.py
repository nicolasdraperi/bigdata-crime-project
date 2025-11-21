# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    weekofyear,
    month,
    count,
    avg,
)


def hdfs_path_exists(spark, path):
    """
    Retourne True si le chemin HDFS existe (fichier ou dossier), False sinon.
    """
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    print("Fichier Déja présent !")
    return fs.exists(jpath)


def main():
    curated_path = "hdfs://namenode:9000/datalake/curated/crime"
    analytics_base = "hdfs://namenode:9000/datalake/analytics/crime"

    spark = (
        SparkSession.builder
        .appName("AggregateCrimeStats")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    df = (
        spark.read
        .option("header", "true")
        .csv(curated_path)
    )
    df = df.withColumn("date_occ", to_date(col("date_occ")))
    if "Part 1-2" in df.columns:
        df = df.withColumnRenamed("Part 1-2", "part_1_2")
    area_col = "AREA NAME"

    df_week = (
        df.withColumn("year_int", year(col("date_occ")))
          .withColumn("week", weekofyear(col("date_occ")))
    )

    df_month = (
        df.withColumn("year_int", year(col("date_occ")))
          .withColumn("month", month(col("date_occ")))
    )

    df_year = df.withColumn("year_int", year(col("date_occ")))


    # Moyenne par zone et par jour
    out_avg_day = analytics_base + "/avg_by_area_day"
    if not hdfs_path_exists(spark, out_avg_day):
        daily_counts = (
            df.groupBy(area_col, "date_occ")
            .agg(count("*").alias("n_crimes"))
        )

        avg_daily = (
            daily_counts.groupBy(area_col)
            .agg(avg("n_crimes").alias("avg_crimes_per_day"))
        )

        (
            avg_daily
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(out_avg_day)
        )

    # Moyenne par zone et par semaine
    out_avg_week = analytics_base + "/avg_by_area_week"
    if not hdfs_path_exists(spark, out_avg_week):
        weekly_counts = (
            df_week.groupBy(area_col, "year_int", "week")
            .agg(count("*").alias("n_crimes"))
        )

        avg_weekly = (
            weekly_counts.groupBy(area_col)
            .agg(avg("n_crimes").alias("avg_crimes_per_week"))
        )

        (
            avg_weekly
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(out_avg_week)
        )

    # Moyenne par zone et par mois
    out_avg_month = analytics_base + "/avg_by_area_month"
    if not hdfs_path_exists(spark, out_avg_month):
        monthly_counts = (
            df_month.groupBy(area_col, "year_int", "month")
            .agg(count("*").alias("n_crimes"))
        )

        avg_monthly = (
            monthly_counts.groupBy(area_col)
            .agg(avg("n_crimes").alias("avg_crimes_per_month"))
        )

        (
            avg_monthly
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(out_avg_month)
        )

    # Total de crimes par zone
    out_total_area = analytics_base + "/total_by_area"
    if not hdfs_path_exists(spark, out_total_area):
        total_by_area = (
            df.groupBy(area_col)
            .agg(count("*").alias("n_crimes_total"))
        )

        (
            total_by_area
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(out_total_area)
        )


    # Volume annuel global
    out_crimes_per_year = analytics_base + "/crimes_per_year"
    if not hdfs_path_exists(spark, out_crimes_per_year):
        crimes_per_year = (
            df_year.groupBy("year_int")
            .agg(count("*").alias("n_crimes"))
        )

        (
            crimes_per_year
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(out_crimes_per_year)
        )


    # Volume annuel par zone
    out_crimes_per_year_area = analytics_base + "/crimes_per_year_area"
    if not hdfs_path_exists(spark, out_crimes_per_year_area):
        crimes_per_year_area = (
            df_year.groupBy("year_int", area_col)
            .agg(count("*").alias("n_crimes"))
        )

        (
            crimes_per_year_area
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(out_crimes_per_year_area)
        )

    # Volume annuel par gravité (global)
    if "part_1_2" in df_year.columns:
        out_crimes_per_year_severity = analytics_base + "/crimes_per_year_severity"
        if not hdfs_path_exists(spark, out_crimes_per_year_severity):
            crimes_per_year_severity = (
                df_year.groupBy("year_int", "part_1_2")
                .agg(count("*").alias("n_crimes"))
            )

            (
                crimes_per_year_severity
                .write
                .mode("overwrite")
                .option("header", "true")
                .csv(out_crimes_per_year_severity)
            )

        # Volume annuel par gravité et par zone
        out_crimes_per_year_area_severity = analytics_base + "/crimes_per_year_area_severity"
        if not hdfs_path_exists(spark, out_crimes_per_year_area_severity):
            crimes_per_year_area_severity = (
                df_year.groupBy("year_int", area_col, "part_1_2")
                .agg(count("*").alias("n_crimes"))
            )

            (
                crimes_per_year_area_severity
                .write
                .mode("overwrite")
                .option("header", "true")
                .csv(out_crimes_per_year_area_severity)
            )

    out_heatmap_monthly = analytics_base + "/crime_heatmap_monthly"
    if not hdfs_path_exists(spark, out_heatmap_monthly):
        df_heat = (
            df_month
            .filter(col("LAT").isNotNull() & col("LON").isNotNull())
        )

        heatmap_monthly = (
            df_heat.groupBy("year_int", "month", area_col, "LAT", "LON")
            .agg(count("*").alias("n_crimes"))
        )

        (
            heatmap_monthly
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(out_heatmap_monthly)
        )

    spark.stop()


if __name__ == "__main__":
    main()
