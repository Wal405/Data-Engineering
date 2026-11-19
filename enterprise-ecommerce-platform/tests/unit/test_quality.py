
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)

# Import the actual e-commerce package
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ecommerce.quality import validate_quality


# ---------------------------------------------------
# 1. Spark fixture
# ---------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession.builder.getOrCreate()
    return session


# ---------------------------------------------------
# 2. Mock dataset metadata
# ---------------------------------------------------

@pytest.fixture
def dataset():
    return SimpleNamespace(
        quality_rules={
            "not_null": ["order_id"],
            "non_negative": ["payment_value"],
            "allowed_values": {
                "order_status": ["delivered", "shipped"]
            },
            "range_checks": {
                "payment_value": {"min": 0, "max": 10000}
            },
            "coordinate_ranges": {},
            "unique_keys": ["order_id"],
        }
    )


# ---------------------------------------------------
# 3. Sample DataFrame factory
# ---------------------------------------------------

@pytest.fixture
def make_df(spark):

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("payment_value", DoubleType(), True),
        StructField("order_status", StringType(), True),
        StructField("_is_valid", BooleanType(), True),
    ])

    def create(rows):
        return spark.createDataFrame(rows, schema)

    return create


# ---------------------------------------------------
# TEST 1: Valid order
# ---------------------------------------------------

def test_valid_order(make_df, dataset):

    df = make_df([
        ("ORD001", 150.0, "delivered", True)
    ])

    result = validate_quality(df, dataset).first()

    assert result["_quality_pass"] is True
    assert result["_dq_reasons"] == []


# ---------------------------------------------------
# TEST 2: Missing required order ID
# ---------------------------------------------------

def test_null_order_id(make_df, dataset):

    df = make_df([
        (None, 150.0, "delivered", True)
    ])

    result = validate_quality(df, dataset).first()

    assert result["_quality_pass"] is False
    assert "NULL_OR_BLANK:order_id" in result["_dq_reasons"]


# ---------------------------------------------------
# TEST 3: Negative payment value
# ---------------------------------------------------

def test_negative_payment(make_df, dataset):

    df = make_df([
        ("ORD002", -50.0, "delivered", True)
    ])

    result = validate_quality(df, dataset).first()

    assert result["_quality_pass"] is False
    assert "NEGATIVE_VALUE:payment_value" in result["_dq_reasons"]


# ---------------------------------------------------
# TEST 4: Invalid order status
# ---------------------------------------------------

def test_invalid_order_status(make_df, dataset):

    df = make_df([
        ("ORD003", 200.0, "unknown_status", True)
    ])

    result = validate_quality(df, dataset).first()

    assert result["_quality_pass"] is False
    assert "INVALID_VALUE:order_status" in result["_dq_reasons"]


# ---------------------------------------------------
# TEST 5: Payment outside allowed range
# ---------------------------------------------------

def test_out_of_range_payment(make_df, dataset):

    df = make_df([
        ("ORD004", 15000.0, "delivered", True)
    ])

    result = validate_quality(df, dataset).first()

    assert result["_quality_pass"] is False
    assert "OUT_OF_RANGE:payment_value" in result["_dq_reasons"]


# ---------------------------------------------------
# TEST 6: Duplicate business key
# ---------------------------------------------------

def test_duplicate_order_id(make_df, dataset):

    df = make_df([
        ("ORD005", 100.0, "delivered", True),
        ("ORD005", 200.0, "shipped", True),
    ])

    results = validate_quality(df, dataset).collect()

    assert len(results) == 2

    for row in results:
        assert row["_quality_pass"] is False
        assert "DUPLICATE_KEY:order_id" in row["_dq_reasons"]
