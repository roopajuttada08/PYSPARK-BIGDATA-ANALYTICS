"""
Big Data Processing & Analytics with PySpark
============================================
A scalable data-processing workflow using Apache Spark / PySpark that performs
cleaning, transformation, aggregation and analytical operations on structured
transaction data. Supports local files and AWS S3 paths.

Run locally:
    python pyspark_pipeline.py --input data/transactions_sample.csv

Run against S3 (configure AWS credentials / spark.hadoop props):
    spark-submit pyspark_pipeline.py --input s3://my-bucket/transactions/
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_spark(app_name: str = "BigDataAnalytics") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


def load_data(spark: SparkSession, path: str):
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )
    return df


def clean_and_transform(df):
    """Normalise columns, cast types, drop bad rows and add derived fields."""
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    df = df.withColumn("amount", F.col("amount").cast("double"))
    df = df.withColumn("quantity", F.col("quantity").cast("int"))

    # Lowercase categorical fields for consistent grouping
    for col in ("product", "category", "region"):
        df = df.withColumn(col, F.lower(F.trim(F.col(col))))

    # Remove rows with missing keys
    df = df.dropna(subset=["transaction_id", "amount"])

    # Derived: order date and revenue bucket
    df = df.withColumn("order_date", F.to_date("timestamp"))
    df = df.withColumn(
        "revenue_band",
        F.when(F.col("amount") >= 50000, "high")
        .when(F.col("amount") >= 10000, "medium")
        .otherwise("low"),
    )
    return df


def aggregate_metrics(df):
    """Category and region level aggregations."""
    by_category = (
        df.groupBy("category")
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.count("transaction_id").alias("txns"),
            F.avg("amount").alias("avg_amount"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    by_region = (
        df.groupBy("region")
        .agg(F.sum("amount").alias("total_revenue"))
        .orderBy(F.desc("total_revenue"))
    )
    return by_category, by_region


def analytical_window_ops(df):
    """Window functions: rank products by revenue and compute running totals."""
    product_revenue = df.groupBy("product").agg(F.sum("amount").alias("revenue"))

    w = Window.orderBy(F.desc("revenue"))
    ranked = product_revenue.withColumn("rank", F.rank().over(w))

    total = Window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ranked = ranked.withColumn(
        "cumulative_revenue", F.sum("revenue").over(total)
    )
    return ranked


def run(input_path: str) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = load_data(spark, input_path)
    print(f"Loaded {raw.count()} raw records")

    clean = clean_and_transform(raw)
    by_category, by_region = aggregate_metrics(clean)
    ranked = analytical_window_ops(clean)

    print("\n--- Revenue by Category ---")
    by_category.show(truncate=False)

    print("--- Revenue by Region ---")
    by_region.show(truncate=False)

    print("--- Product Ranking (window: rank + cumulative) ---")
    ranked.show(truncate=False)

    # Persist processed output as Parquet (columnar, big-data friendly)
    clean.write.mode("overwrite").parquet("output/processed")
    print("Processed data written to output/processed (Parquet)")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark big-data analytics pipeline")
    parser.add_argument(
        "--input",
        default="data/transactions_sample.csv",
        help="Path to input CSV (local or s3://)",
    )
    args = parser.parse_args()
    run(args.input)
