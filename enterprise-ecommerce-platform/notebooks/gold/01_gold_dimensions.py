# Databricks notebook source

# PHASE 5 - GOLD DIMENSION LOADING

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

results = []

for name in ("customers", "products", "sellers"):
    ds = registry.get_dataset(name)

    try:
        action = gold.build_dimension(spark, ds)

        results.append({
            "dimension": f"dim_{name}",
            "action": action,
            "status": "SUCCESS",
            "error": ""
        })

    except Exception as e:
        results.append({
            "dimension": f"dim_{name}",
            "action": "FAILED",
            "status": "FAILED",
            "error": str(e)[:500]
        })

display(spark.createDataFrame(results))

if any(r["status"] == "FAILED" for r in results):
    raise RuntimeError("Gold dimension loading failed")


# COMMAND ----------


# PHASE 5 - GOLD DIMENSION VALIDATION

import sys
from pathlib import Path
from pyspark.sql import functions as F

# Locate project
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
    raise RuntimeError("Project root not found")

sys.path.insert(0, str(project_root / "src"))

from ecommerce.metadata import MetadataRegistry

registry = MetadataRegistry.from_yaml(
    project_root / "config/dev.yml"
)

results = []

for name in ("customers", "products", "sellers"):

    ds = registry.get_dataset(name)
    key = list(ds.business_keys)[0]

    catalog = ds.config["platform"]["catalog"]
    gold_schema = ds.config["platform"]["schemas"]["gold"]
    gold_table = f"{catalog}.{gold_schema}.dim_{name}"

    silver = spark.table(ds.full_silver_table)
    gold = spark.table(gold_table)

    silver_rows = silver.count()
    gold_rows = gold.count()

    # SCD2 current versions
    current = gold.filter(F.col("is_current") == True)

    duplicate_version_keys = (
        gold.groupBy("dimension_key")
        .count()
        .filter("count > 1")
        .count()
    )

    duplicate_current_keys = (
        current.groupBy(key)
        .count()
        .filter("count > 1")
        .count()
    )

    invalid_history = gold.filter(
        F.col("dimension_key").isNull()
        | F.col("effective_from").isNull()
        | F.col("is_current").isNull()
        | (
            (F.col("is_current") == True)
            & F.col("effective_to").isNotNull()
        )
        | (
            (F.col("is_current") == False)
            & F.col("effective_to").isNull()
        )
    ).count()

    silver_current = silver.filter(
        F.col("is_current") == True
    ).count()

    passed = (
        silver_rows == gold_rows
        and current.count() == silver_current
        and duplicate_version_keys == 0
        and duplicate_current_keys == 0
        and invalid_history == 0
    )

    results.append({
        "dimension": f"dim_{name}",
        "silver_rows": silver_rows,
        "gold_rows": gold_rows,
        "current_rows": current.count(),
        "duplicate_versions": duplicate_version_keys,
        "duplicate_current_keys": duplicate_current_keys,
        "invalid_history": invalid_history,
        "status": "PASS" if passed else "FAIL"
    })

display(spark.createDataFrame(results))

failed = [r for r in results if r["status"] != "PASS"]

if failed:
    raise RuntimeError(
        "Gold dimension validation failed: "
        + ", ".join(r["dimension"] for r in failed)
    )

print("SUCCESS: All 3 Gold dimensions validated.")


# COMMAND ----------


# PHASE 5 - BUILD GOLD DATE DIMENSION

import sys
import importlib
from pathlib import Path

# Locate the current notebook
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)

notebook_file = Path("/Workspace") / notebook_path.lstrip("/")

# Find project root
project_root = next(
    (
        p for p in notebook_file.parents
        if p.name == "enterprise-ecommerce-platform"
    ),
    None
)

if project_root is None:
    raise RuntimeError(
        f"Project folder not found: {notebook_file}"
    )

# Make the ecommerce package importable
src_path = project_root / "src"

if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Import project modules
from ecommerce.metadata import MetadataRegistry

gold_module = importlib.import_module("ecommerce.gold")
importlib.reload(gold_module)

registry = MetadataRegistry.from_yaml(
    project_root / "config/dev.yml"
)

# Build date dimension
platform = registry.get_dataset("orders").config["platform"]

action = gold_module.build_dim_date(
    spark,
    platform["catalog"],
    platform["schemas"]["silver"],
    platform["schemas"]["gold"]
)

print(f"dim_date: {action}")
