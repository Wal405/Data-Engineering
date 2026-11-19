# Databricks notebook source

# PHASE 5 - FINAL GOLD LAYER VALIDATION

from pyspark.sql import functions as F

catalog = "ecommerce_dev"
silver = f"{catalog}.silver"
gold = f"{catalog}.gold"

results = []

def record(check, failures, details=""):
    results.append({
        "check": check,
        "status": "PASS" if failures == 0 else "FAIL",
        "failures": int(failures),
        "details": str(details)
    })

def duplicate_count(df, keys):
    return (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def missing_references(child_table, child_key,
                       parent_table, parent_key):

    child = spark.table(f"{gold}.{child_table}")
    parent = spark.table(f"{gold}.{parent_table}")

    return (
        child.alias("c")
        .join(
            parent.select(parent_key).distinct().alias("p"),
            F.col(f"c.{child_key}") == F.col(f"p.{parent_key}"),
            "left_anti"
        )
        .count()
    )


def total(table, column):
    return (
        spark.table(table)
        .agg(F.sum(column).alias("amount"))
        .first()["amount"]
    )

# 1. Dimension validation
for name in ("customers", "products", "sellers"):

    source = spark.table(f"{silver}.silver_{name}")
    target = spark.table(f"{gold}.dim_{name}")

    business_key = {
        "customers": "customer_id",
        "products": "product_id",
        "sellers": "seller_id"
    }[name]

    record(
        f"dim_{name}: row count",
        abs(source.count() - target.count())
    )

    record(
        f"dim_{name}: unique dimension keys",
        duplicate_count(target, ["dimension_key"])
    )

    current = target.filter(F.col("is_current") == True)

    record(
        f"dim_{name}: current key uniqueness",
        duplicate_count(current, [business_key])
    )

    record(
        f"dim_{name}: current row count",
        abs(
            source.filter("is_current = true").count()
            - current.count()
        )
    )

    record(
        f"dim_{name}: history integrity",
        target.filter(
            F.col("dimension_key").isNull()
            | F.col("effective_from").isNull()
            | F.col("is_current").isNull()
            | (
                (F.col("is_current") == True)
                & F.col("effective_to").isNotNull()
            )
            | (
                (F.col("is_current") == False)
                & (
                    F.col("effective_to").isNull()
                    | (F.col("effective_to")
                       < F.col("effective_from"))
                )
            )
        ).count()
    )

# 2. Fact table grain and reconciliation
facts = {
    "orders": (["order_id"], "orders"),
    "order_items": (
        ["order_id", "order_item_id"], "order_items"
    ),
    "payments": (
        ["order_id", "payment_sequential"], "order_payments"
    )
}

for name, (keys, source_name) in facts.items():

    source = spark.table(f"{silver}.silver_{source_name}")
    target = spark.table(f"{gold}.fact_{name}")

    record(
        f"fact_{name}: row count",
        abs(source.count() - target.count())
    )

    record(
        f"fact_{name}: unique grain",
        duplicate_count(target, keys)
    )

# 3. Dimension reference integrity
relationships = [
    ("fact_orders", "customer_dimension_key",
     "dim_customers", "dimension_key"),

    ("fact_order_items", "customer_dimension_key",
     "dim_customers", "dimension_key"),

    ("fact_order_items", "product_dimension_key",
     "dim_products", "dimension_key"),

    ("fact_order_items", "seller_dimension_key",
     "dim_sellers", "dimension_key"),

    ("fact_payments", "customer_dimension_key",
     "dim_customers", "dimension_key")
]

for child, child_key, parent, parent_key in relationships:
    record(
        f"{child} -> {parent}",
        missing_references(
            child, child_key, parent, parent_key
        )
    )

# 4. Date dimension validation
dates = spark.table(f"{gold}.dim_date")

record(
    "dim_date: unique dates",
    duplicate_count(dates, ["date_key"])
)

for name in facts:
    fact = spark.table(f"{gold}.fact_{name}")

    # Missing source dates are reported separately.
    # Non-null dates must resolve to dim_date.
    unmatched = (
        fact.filter(F.col("order_date_key").isNotNull())
        .select("order_date_key")
        .join(
            dates.select("date_key"),
            F.col("order_date_key") == F.col("date_key"),
            "left_anti"
        ).count()
    )

    record(f"fact_{name}: date references", unmatched)

# 5. Financial reconciliation
for fact_name, source_name, measures in [
    ("order_items", "order_items",
     ["price", "freight_value"]),
    ("payments", "order_payments",
     ["payment_value"])
]:
    for measure in measures:
        source_total = total(
            f"{silver}.silver_{source_name}", measure
        )
        gold_total = total(
            f"{gold}.fact_{fact_name}", measure
        )

        record(
            f"{fact_name}: {measure} reconciliation",
            0 if source_total == gold_total else 1,
            f"Silver={source_total}; Gold={gold_total}"
        )

# Final results
display(spark.createDataFrame(results))

failed = [r for r in results if r["status"] == "FAIL"]

print(f"Total checks: {len(results)}")
print(f"Passed: {len(results) - len(failed)}")
print(f"Failed: {len(failed)}")

if failed:
    raise RuntimeError(
        "Gold validation failed: "
        + ", ".join(r["check"] for r in failed)
    )

print("SUCCESS: Gold Layer validation passed.")
