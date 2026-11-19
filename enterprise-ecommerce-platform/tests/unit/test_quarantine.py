
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import ecommerce.quality as quality


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.getActiveSession()
    if session is None:
        session = SparkSession.builder.getOrCreate()
    return session


@pytest.fixture
def dataset():
    return SimpleNamespace(
        name="orders",
        business_keys=["order_id"],
        full_bronze_table="ecommerce_dev.bronze.orders",
        full_silver_table="ecommerce_dev.silver.orders"
    )


@pytest.fixture
def make_df(spark):
    schema = """
        order_id STRING,
        _is_valid BOOLEAN,
        _quality_pass BOOLEAN,
        _dq_reasons ARRAY<STRING>,
        _raw_record_json STRING
    """

    def create(rows):
        return spark.createDataFrame(rows, schema)

    return create


# TEST 1: Missing raw record column
def test_missing_raw_record(spark, dataset):

    df = spark.createDataFrame(
        [("O101", True)],
        "order_id STRING, _quality_pass BOOLEAN"
    )

    with pytest.raises(
        ValueError,
        match="Missing _raw_record_json"
    ):
        quality.quarantine_invalid_records(
            spark, df, dataset, "test-run"
        )


# TEST 2: All records are valid
def test_no_rejected_records(
    spark, dataset, make_df, monkeypatch
):

    df = make_df([
        ("O101", True, True, [], '{"order_id":"O101"}')
    ])

    # Prevent creation of the real quarantine table
    mock_ensure = MagicMock()
    monkeypatch.setattr(
        quality, "ensure_quarantine_table", mock_ensure
    )

    valid_df, rejected_count = (
        quality.quarantine_invalid_records(
            spark, df, dataset, "test-run"
        )
    )

    assert rejected_count == 0
    assert valid_df.count() == 1
    assert "_quality_pass" not in valid_df.columns

    mock_ensure.assert_called_once()


# TEST 3: Invalid record is quarantined
def test_invalid_record(
    spark, dataset, make_df, monkeypatch
):

    df = make_df([
        ("O101", True, True, [], '{"order_id":"O101"}'),
        (
            "O102", True, False,
            ["INVALID_VALUE:order_status"],
            '{"order_id":"O102"}'
        )
    ])

    # Mock table creation
    monkeypatch.setattr(
        quality,
        "ensure_quarantine_table",
        MagicMock()
    )

    # Mock Delta MERGE so no data is written
    mock_delta = MagicMock()

    mock_for_name = MagicMock(return_value=mock_delta)

    monkeypatch.setattr(
        quality.DeltaTable,
        "forName",
        mock_for_name
    )

    valid_df, rejected_count = (
        quality.quarantine_invalid_records(
            spark, df, dataset, "test-run"
        )
    )

    assert rejected_count == 1
    assert valid_df.count() == 1
    assert valid_df.first()["order_id"] == "O101"

    mock_delta.alias.assert_called_once()
    mock_delta.alias.return_value.merge.assert_called_once()


