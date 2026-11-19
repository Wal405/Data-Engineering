
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ecommerce.quality import check_foreign_keys


# ---------------------------------------------------
# 1. Spark session fixture
# ---------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession.builder.getOrCreate()
    return session


# ---------------------------------------------------
# 2. Fake dataset configuration
# ---------------------------------------------------

@pytest.fixture
def dataset():
    return SimpleNamespace(
        quality_rules={
            "foreign_keys": [
                {
                    "column": "customer_id",
                    "ref_table": "customers",
                    "ref_column": "customer_id"
                }
            ]
        },
        config={
            "platform": {
                "catalog": "ecommerce_dev",
                "schemas": {
                    "silver": "silver"
                }
            }
        }
    )


# ---------------------------------------------------
# 3. Mock the external Spark table access
# ---------------------------------------------------

@pytest.fixture
def mock_spark(spark):
    parent_df = spark.createDataFrame(
        [("C101",), ("C102",)],
        ["customer_id"]
    )

    fake_spark = MagicMock()
    fake_spark.catalog.tableExists.return_value = True
    fake_spark.table.return_value = parent_df

    return fake_spark


# ---------------------------------------------------
# TEST 1: Existing parent customer
# ---------------------------------------------------

def test_valid_foreign_key(spark, mock_spark, dataset):

    orders_df = spark.createDataFrame(
        [("O101", "C101", [])],
        "order_id STRING, customer_id STRING, "
        "_dq_reasons ARRAY<STRING>"
    )

    result = check_foreign_keys(
        mock_spark, orders_df, dataset
    ).first()

    assert result["_quality_pass"] is True
    assert result["_dq_reasons"] == []


# ---------------------------------------------------
# TEST 2: Missing parent customer
# ---------------------------------------------------

def test_invalid_foreign_key(spark, mock_spark, dataset):

    orders_df = spark.createDataFrame(
        [("O102", "C999", [])],
        "order_id STRING, customer_id STRING, "
        "_dq_reasons ARRAY<STRING>"
    )

    result = check_foreign_keys(
        mock_spark, orders_df, dataset
    ).first()

    assert result["_quality_pass"] is False

    assert (
        "FK_VIOLATION:customer_id->customers.customer_id"
        in result["_dq_reasons"]
    )


# ---------------------------------------------------
# TEST 3: Parent table does not exist
# ---------------------------------------------------

def test_missing_parent_table(spark, mock_spark, dataset):

    mock_spark.catalog.tableExists.return_value = False

    orders_df = spark.createDataFrame(
        [("O103", "C101", [])],
        "order_id STRING, customer_id STRING, "
        "_dq_reasons ARRAY<STRING>"
    )

    with pytest.raises(
        RuntimeError,
        match="Parent Silver table missing"
    ):
        check_foreign_keys(
            mock_spark, orders_df, dataset
        )
