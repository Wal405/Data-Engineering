
# src/ecommerce/monitoring.py

import hashlib
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, LongType
)

AUDIT_TABLE = "ecommerce_dev.monitoring.pipeline_audit"


def log_pipeline_audit(
    spark,
    run_id,
    dataset_name,
    layer,
    source_table,
    target_table,
    started_at,
    execution_status,
    records_read=None,
    records_written=None,
    records_rejected=None,
    error_message=None,
    audit_table=AUDIT_TABLE
):
    """Insert or update one dataset execution audit record."""

    if not run_id:
        raise ValueError("A nonempty run_id is required")

    # Stable identifier for retries of the same dataset/layer/run
    audit_id = hashlib.sha256(
        f"{run_id}|{dataset_name}|{layer}".encode("utf-8")
    ).hexdigest()

    now = datetime.now(timezone.utc)

    schema = StructType([
        StructField("audit_id", StringType(), False),
        StructField("run_id", StringType()),
        StructField("dataset_name", StringType()),
        StructField("layer", StringType()),
        StructField("source_table", StringType()),
        StructField("target_table", StringType()),
        StructField("started_at", TimestampType()),
        StructField("completed_at", TimestampType()),
        StructField("records_read", LongType()),
        StructField("records_written", LongType()),
        StructField("records_rejected", LongType()),
        StructField("execution_status", StringType()),
        StructField("error_message", StringType()),
        StructField("logged_at", TimestampType())
    ])

    row = [(
        audit_id,
        str(run_id),
        dataset_name,
        layer,
        source_table,
        target_table,
        started_at,
        now,
        records_read,
        records_written,
        records_rejected,
        execution_status,
        error_message,
        now
    )]

    source_df = spark.createDataFrame(row, schema)

    (
        DeltaTable.forName(spark, audit_table)
        .alias("target")
        .merge(
            source_df.alias("source"),
            "target.audit_id = source.audit_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    return audit_id
