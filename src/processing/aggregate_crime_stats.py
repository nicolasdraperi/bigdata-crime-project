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


    # Average crimes per area per day
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
        .csv(analytics_base + "/avg_by_area_day")
    )

    # Average crimes per area per week
    df_week = (
        df.withColumn("year_int", year(col("date_occ")))
          .withColumn("week", weekofyear(col("date_occ")))
    )

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
        .csv(analytics_base + "/avg_by_area_week")
    )

    # Average crimes per area per month
    df_month = (
        df.withColumn("year_int", year(col("date_occ")))
          .withColumn("month", month(col("date_occ")))
    )

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
        .csv(analytics_base + "/avg_by_area_month")
    )

    # Total number of crimes per area
    total_by_area = (
        df.groupBy(area_col)
        .agg(count("*").alias("n_crimes_total"))
    )

    (
        total_by_area
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(analytics_base + "/total_by_area")
    )

    # Yearly crime volume
    df_year = df.withColumn("year_int", year(col("date_occ")))

    crimes_per_year = (
        df_year.groupBy("year_int")
        .agg(count("*").alias("n_crimes"))
    )

    (
        crimes_per_year
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(analytics_base + "/crimes_per_year")
    )

    # Yearly crime volume per area
    crimes_per_year_area = (
        df_year.groupBy("year_int", area_col)
        .agg(count("*").alias("n_crimes"))
    )

    (
        crimes_per_year_area
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(analytics_base + "/crimes_per_year_area")
    )
    

    # Yearly crime volume by severity (global)
    if "part_1_2" in df_year.columns:
        crimes_per_year_severity = (
            df_year.groupBy("year_int", "part_1_2")
            .agg(count("*").alias("n_crimes"))
        )

        (
            crimes_per_year_severity
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(analytics_base + "/crimes_per_year_severity")
        )

        # Yearly crime volume by severity and area
        crimes_per_year_area_severity = (
            df_year.groupBy("year_int", area_col, "part_1_2")
            .agg(count("*").alias("n_crimes"))
        )

        (
            crimes_per_year_area_severity
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(analytics_base + "/crimes_per_year_area_severity")
        )

    # Heatmap monthly data (year, month, area, lat, lon, count)
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
        .csv(analytics_base + "/crime_heatmap_monthly")
    )

    spark.stop()


if __name__ == "__main__":
    main()
