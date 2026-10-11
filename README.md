# ⚡ Big Data Processing & Analytics with PySpark

A scalable **big-data processing workflow** built with **Apache Spark** and
**PySpark** that cleans, transforms, aggregates and analyses large structured
datasets — with integration for **AWS S3** cloud storage.

## 🎯 Objectives

- Process large datasets efficiently using distributed computation
- Implement cleaning, transformation, aggregation and analytical operations
- Explore scalable processing and cloud (AWS S3) storage integration

## ✨ Features

- 🧹 **Cleaning & transformation** — type casting, trimming, null handling
- 📊 **Aggregations** — revenue by category and region
- 🔢 **Window functions** — product ranking (`RANK`) and cumulative running totals
- 🪵 **Derived features** — order date and revenue bands
- 💾 **Parquet output** — columnar format optimised for analytics
- ☁️ **S3-ready** — read inputs from `s3://` paths or local files

## 🧱 Tech Stack

`Python` · `Apache Spark` · `PySpark` · `SQL` · `AWS S3`

## 🚀 Getting Started

**Prerequisites:** Java 8/11/17 and Python 3.

```bash
# install
pip install -r requirements.txt

# run locally
python pyspark_pipeline.py --input data/transactions_sample.csv

# or via spark-submit
spark-submit pyspark_pipeline.py --input data/transactions_sample.csv
```

### Using AWS S3 as input

```bash
spark-submit \
  --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
  --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
  pyspark_pipeline.py --input s3://your-bucket/transactions/
```

## 📂 Project Structure

```
PySpark-BigData-Analytics/
├── data/transactions_sample.csv
├── pyspark_pipeline.py     # Spark ETL + analytics job
├── requirements.txt
└── README.md
```

## 🧠 What it demonstrates

| Concept | Where |
|---------|-------|
| Distributed processing | `SparkSession`, DataFrame ops |
| Transformations | `clean_and_transform()` |
| Aggregations | `aggregate_metrics()` |
| Window functions | `analytical_window_ops()` |
| Cloud storage | S3 input support |

---

Built by [Roopa Juttada](https://github.com/roopajuttada08) 🚀
