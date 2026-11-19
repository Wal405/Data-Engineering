# Databricks notebook source

# PHASE 5 - GOLD FACT TABLE LOADING

import sys
import importlib
from pathlib import Path

notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)

notebook_file = Path("/Workspace") / notebook_path.lstrip("/")

project_root = next(
    (p for p in notebook_file.parents
     if p.name == "enterprise-ecommerce-platform"),
    None
)

if project_root is None:
    raise RuntimeError(f"Project root not found: {notebook_file}")

sys.path.insert(0, str(project_root / "src"))

from ecommerce.metadata import MetadataRegistry
import ecommerce.gold as gold

importlib.reload(gold)

registry = MetadataRegistry.from_yaml(
    project_root / "config/dev.yml"
)

platform = registry.get_dataset("orders").config["platform"]

catalog = platform["catalog"]
silver_schema = platform["schemas"]["silver"]
gold_schema = platform["schemas"]["gold"]

# Build order-level fact table
action = gold.build_fact_orders(
    spark, catalog, silver_schema, gold_schema
)

print(f"fact_orders: {action}")


# COMMAND ----------


# VALIDATE FACT ORDERS

from pyspark.sql import functions as F

silver = spark.table(
    f"{catalog}.{silver_schema}.silver_orders"
)

fact = spark.table(
    f"{catalog}.{gold_schema}.fact_orders"
)

silver_count = silver.count()
fact_count = fact.count()

duplicate_orders = (
    fact.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

missing_customers = fact.filter(
    F.col("customer_dimension_key").isNull()
).count()

missing_dates = (
    fact.filter(F.col("order_date_key").isNotNull())
    .select("order_date_key").distinct()
    .join(
        spark.table(f"{catalog}.{gold_schema}.dim_date")
        .select("date_key"),
        F.col("order_date_key") == F.col("date_key"),
        "left_anti"
    )
    .count()
)

assert silver_count == fact_count, "Order count mismatch"
assert duplicate_orders == 0, "Duplicate orders detected"
assert missing_customers == 0, "Missing customer keys"
assert missing_dates == 0, "Date keys not found in dim_date"

print("Silver Orders:", silver_count)
print("Gold Fact Orders:", fact_count)
print("Duplicate Orders:", duplicate_orders)
print("Missing Customer Keys:", missing_customers)
print("Unmatched Date Keys:", missing_dates)
print("SUCCESS: fact_orders validated")


# COMMAND ----------


# PHASE 5 - BUILD FACT ORDER ITEMS

action = gold.build_fact_order_items(
    spark,
    catalog,
    silver_schema,
    gold_schema
)

print(f"fact_order_items: {action}")


# COMMAND ----------


# PHASE 5 - VALIDATE FACT ORDER ITEMS

from pyspark.sql import functions as F

silver = spark.table(
    f"{catalog}.{silver_schema}.silver_order_items"
)

fact = spark.table(
    f"{catalog}.{gold_schema}.fact_order_items"
)

silver_count = silver.count()
fact_count = fact.count()

duplicate_items = (
    fact.groupBy("order_id", "order_item_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

missing_dimensions = fact.filter(
    F.col("customer_dimension_key").isNull()
    | F.col("product_dimension_key").isNull()
    | F.col("seller_dimension_key").isNull()
).count()

silver_totals = silver.agg(
    F.sum("price").alias("price"),
    F.sum("freight_value").alias("freight")
).first()

gold_totals = fact.agg(
    F.sum("price").alias("price"),
    F.sum("freight_value").alias("freight")
).first()

assert silver_count == fact_count, "Row count mismatch"
assert duplicate_items == 0, "Duplicate order items"
assert missing_dimensions == 0, "Missing dimension keys"

assert silver_totals["price"] == gold_totals["price"]
assert silver_totals["freight"] == gold_totals["freight"]

print("Silver rows:", silver_count)
print("Gold rows:", fact_count)
print("Duplicate items:", duplicate_items)
print("Missing dimension keys:", missing_dimensions)
print("SUCCESS: fact_order_items validated")


# COMMAND ----------


# PHASE 5 - BUILD FACT PAYMENTS

action = gold.build_fact_payments(
    spark,
    catalog,
    silver_schema,
    gold_schema
)

print(f"fact_payments: {action}")


# COMMAND ----------


# PHASE 5 - VALIDATE FACT PAYMENTS

from pyspark.sql import functions as F

silver = spark.table(
    f"{catalog}.{silver_schema}.silver_order_payments"
)

fact = spark.table(
    f"{catalog}.{gold_schema}.fact_payments"
)

silver_count = silver.count()
fact_count = fact.count()

duplicate_payments = (
    fact.groupBy("order_id", "payment_sequential")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

missing_keys = fact.filter(
    F.col("customer_dimension_key").isNull()
    | F.col("order_date_key").isNull()
).count()

silver_total = silver.agg(
    F.sum("payment_value").alias("total")
).first()["total"]

gold_total = fact.agg(
    F.sum("payment_value").alias("total")
).first()["total"]

results = {
    "silver_rows": silver_count,
    "gold_rows": fact_count,
    "duplicate_payments": duplicate_payments,
    "missing_dimension_keys": missing_keys,
    "silver_payment_total": silver_total,
    "gold_payment_total": gold_total
}

for name, value in results.items():
    print(f"{name}: {value}")

assert silver_count == fact_count, "Row count mismatch"
assert duplicate_payments == 0, "Duplicate payments found"
assert missing_keys == 0, "Missing dimension keys"
assert silver_total == gold_total, "Payment totals mismatch"

print("SUCCESS: fact_payments validated")
