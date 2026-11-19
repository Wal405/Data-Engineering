
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ecommerce.transformations import prepare_silver


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession.builder.getOrCreate()
    return session


@pytest.fixture
def dataset():
    return SimpleNamespace(
        metadata={
            "columns": [
                {
                    "name": "order_id",
                    "type": "STRING",
                    "nullable": False
                },
                {
                    "name": "payment_value",
                    "type": "DOUBLE",
                    "nullable": True
                }
            ]
        }
    )


# TEST 1: Valid data conversion
def test_valid_cast(spark, dataset):
    df = spark.createDataFrame(
        [("O101", "150.50")],
        ["order_id", "payment_value"]
    )

    result = prepare_silver(df, dataset).first()

    assert result["payment_value"] == 150.50
    assert result["_is_valid"] is True


# TEST 2: Invalid numeric value
def test_invalid_cast(spark, dataset):
    df = spark.createDataFrame(
        [("O102", "invalid")],
        ["order_id", "payment_value"]
    )

    result = prepare_silver(df, dataset).first()

    assert result["payment_value"] is None
    assert result["_is_valid"] is False


# TEST 3: Required column is NULL
def test_required_null(spark, dataset):
    df = spark.createDataFrame(
        [(None, "200.00")],
        "order_id STRING, payment_value STRING"
    )

    result = prepare_silver(df, dataset).first()

    assert result["order_id"] is None
    assert result["_is_valid"] is False


# TEST 4: Nullable column is NULL
def test_nullable_column(spark, dataset):
    df = spark.createDataFrame(
        [("O103", None)],
        "order_id STRING, payment_value STRING"
    )

    result = prepare_silver(df, dataset).first()

    assert result["payment_value"] is None
    assert result["_is_valid"] is True


# TEST 5: Verify converted data type
def test_schema_conversion(spark, dataset):
    df = spark.createDataFrame(
        [("O104", "99.90")],
        ["order_id", "payment_value"]
    )

    result = prepare_silver(df, dataset)

    assert isinstance(
        result.schema["payment_value"].dataType,
        DoubleType
    )


# TEST 6: Preserve source metadata
def test_metadata_preserved(spark, dataset):
    df = spark.createDataFrame(
        [("O105", "100.00", "orders.csv")],
        ["order_id", "payment_value", "_source_file"]
    )

    result = prepare_silver(df, dataset)

    assert "_source_file" in result.columns
    assert result.first()["_source_file"] == "orders.csv"
