# -*- coding: utf-8 -*-
# ^ j'ai pas compris ce truc mais ça regle mes probleme 

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    count,
    avg,
    stddev_pop,
    floor,
    lit,
)


def find_col(df, candidates_normalized):
    for c in df.columns:
        norm = c.lower().replace(" ", "").replace("_", "")
        if norm in candidates_normalized:
            return c
    return None


def build_time_slot(hour_col):
    return (
        when((hour_col >= 0) & (hour_col < 6), "Nuit (0-5)")
        .when((hour_col >= 6) & (hour_col < 12), "Matin (6-11)")
        .when((hour_col >= 12) & (hour_col < 18), "Apres-midi (12-17)")
        .when((hour_col >= 18) & (hour_col <= 23), "Soiree (18-23)")
        .otherwise("Inconnu")
    )


def build_age_range(age_col):
    return (
        when(age_col.isNull() | (age_col <= 0), "Inconnu")
        .when(age_col < 18, "0-17")
        .when(age_col < 30, "18-29")
        .when(age_col < 45, "30-44")
        .when(age_col < 65, "45-64")
        .otherwise("65+")
    )


def hdfs_path_exists(spark, path):
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
        .appName("DetectHighRiskContexts")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )

    out_scores = analytics_base + "/context_risk_scores"
    out_high_risk = analytics_base + "/high_risk_contexts"

    if hdfs_path_exists(spark, out_scores) and hdfs_path_exists(spark, out_high_risk):
        print("Context risk outputs already exist in HDFS, skipping computation.")
        spark.stop()
        return

    df = (
        spark.read
        .option("header", "true")
        .csv(curated_path)
    )

    area_col = find_col(df, {"areaname"}) or "AREA NAME"
    time_col = find_col(df, {"timeocc"})
    age_col_name = find_col(df, {"victage"})
    sex_col_name = find_col(df, {"victsex"})
    premis_col_name = find_col(df, {"premisdesc"})

    if not all([area_col, time_col, age_col_name, sex_col_name, premis_col_name]):
        print("Colonnes manquantes pour la detection de contexte :")
        print("area_col:", area_col)
        print("time_col:", time_col)
        print("age_col_name:", age_col_name)
        print("sex_col_name:", sex_col_name)
        print("premis_col_name:", premis_col_name)
        spark.stop()
        return

    # Cast de base
    df_ctx = (
        df
        .withColumn("time_int", col(time_col).cast("int"))
        .withColumn("vict_age_int", col(age_col_name).cast("int"))
    )

    # Type de victime : humain vs non humain
    victim_type = when(
        (col("vict_age_int") > 0) | (col(sex_col_name).isin("M", "F")),
        "Humain"
    ).otherwise("NonHumain")

    df_ctx = df_ctx.withColumn("victim_type", victim_type)

    # On ne garde que les victimes humaines pour l'analyse contextuelle
    df_ctx = df_ctx.filter(col("victim_type") == "Humain")

    # Tranche horaire
    hour_col = floor(col("time_int") / 100)
    df_ctx = df_ctx.withColumn("time_slot", build_time_slot(hour_col))

    # Tranche d'age
    df_ctx = df_ctx.withColumn("age_range", build_age_range(col("vict_age_int")))

    # Sexe normalise
    sex_norm = (
        when(col(sex_col_name).isNull(), "Inconnu")
        .when(col(sex_col_name) == "M", "M")
        .when(col(sex_col_name) == "F", "F")
        .otherwise("Autre")
    )
    df_ctx = df_ctx.withColumn("sex_norm", sex_norm)

    # Type de lieu
    df_ctx = df_ctx.withColumnRenamed(premis_col_name, "place_type")

    # Filtre de securite sur les champs de contexte
    df_ctx = df_ctx.filter(
        col(area_col).isNotNull()
        & col("time_slot").isNotNull()
        & col("place_type").isNotNull()
        & col("sex_norm").isNotNull()
    )

    # ------------- Aggregation par contexte -------------
    group_cols = [area_col, "time_slot", "place_type", "age_range", "sex_norm"]

    context_counts = (
        df_ctx.groupBy(*group_cols)
        .agg(count("*").alias("n_crimes"))
    )

    stats = context_counts.agg(
        avg("n_crimes").alias("mean_n"),
        stddev_pop("n_crimes").alias("std_n"),
    ).collect()[0]

    mean_n = stats["mean_n"]
    std_n = stats["std_n"] if stats["std_n"] is not None and stats["std_n"] != 0 else 1.0

    scored = context_counts.withColumn(
        "z_score", (col("n_crimes") - lit(mean_n)) / lit(std_n)
    )

    scored = scored.withColumn(
        "risk_level",
        when(col("z_score") >= 3.0, "Tres eleve")
        .when(col("z_score") >= 2.0, "Eleve")
        .otherwise("Normal"),
    )

    min_count = 50

    high_risk = (
        scored.filter((col("n_crimes") >= min_count) & (col("z_score") >= 2.0))
        .orderBy(col("z_score").desc())
    )

    if not hdfs_path_exists(spark, out_scores):
        scored.write.mode("overwrite").option("header", "true") \
            .csv(out_scores)

    if not hdfs_path_exists(spark, out_high_risk):
        high_risk.write.mode("overwrite").option("header", "true") \
            .csv(out_high_risk)

    spark.stop()


if __name__ == "__main__":
    main()
