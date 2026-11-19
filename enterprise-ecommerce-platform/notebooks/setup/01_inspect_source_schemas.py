# Databricks notebook source

from pyspark.sql import functions as F

# ============================================================
# PHASE 2 - CHECK ALL NINE OLIST CSV SCHEMAS
# ============================================================

source_path = "/Volumes/ecommerce_dev/bronze/raw_olist_source"

files = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "product_category_translation":
        "product_category_name_translation.csv"
}

schema_summary = []

for dataset_name, file_name in files.items():

    file_path = f"{source_path}/{file_name}"

    print("=" * 70)
    print(f"DATASET: {dataset_name}")
    print(f"FILE: {file_name}")
    print("=" * 70)

    try:
        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .load(file_path)
        )

        # Display inferred schema
        df.printSchema()

        # Count records and columns
        row_count = df.count()
        column_count = len(df.columns)

        print(f"Total Rows: {row_count}")
        print(f"Total Columns: {column_count}")

        # Store schema information
        for field in df.schema.fields:
            schema_summary.append({
                "dataset": dataset_name,
                "column_name": field.name,
                "data_type": field.dataType.simpleString(),
                "nullable": field.nullable
            })

        print(f"SUCCESS: {dataset_name}")

    except Exception as e:
        print(f"ERROR reading {dataset_name}: {e}")

# ============================================================
# COMBINED SCHEMA SUMMARY
# ============================================================

if schema_summary:
    summary_df = spark.createDataFrame(schema_summary)

    display(
        summary_df.orderBy("dataset", "column_name")
    )
