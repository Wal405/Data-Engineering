# Databricks notebook source
# PHASE 6 - JOB EXECUTION MONITORING

from pyspark.sql import functions as F
from pyspark.sql.window import Window

job_name = "enterprise_ecommerce_dev_pipeline"

# 1. Identify the current job
jobs = spark.table("system.lakeflow.jobs")

latest_jobs = (
    jobs.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("workspace_id", "job_id")
                  .orderBy(F.col("change_time").desc())
        )
    )
    .filter(F.col("_rn") == 1)
    .filter(F.col("delete_time").isNull())
    .filter(F.col("name") == job_name)
    .select("workspace_id", "job_id", "name")
)

if latest_jobs.limit(1).count() == 0:
    raise RuntimeError(
        f"Job not found in system tables: {job_name}"
    )

# 2. Read task execution history
timeline = spark.table(
    "system.lakeflow.job_task_run_timeline"
)

# 3. Aggregate hourly timeline slices into task runs
task_runs = (
    timeline.join(
        latest_jobs,
        ["workspace_id", "job_id"],
        "inner"
    )
    .filter(
        F.col("period_start_time")
        >= F.current_timestamp() - F.expr("INTERVAL 30 DAYS")
    )
    .groupBy(
        "workspace_id",
        "job_id",
        "name",
        "job_run_id",
        "run_id",
        "task_key"
    )
    .agg(
        F.min("period_start_time").alias("start_time"),
        F.max("period_end_time").alias("end_time"),
        F.sum(
            F.unix_timestamp("period_end_time")
            - F.unix_timestamp("period_start_time")
        ).alias("duration_seconds"),
        F.max_by(
            F.col("result_state"),
            F.col("period_end_time")
        ).alias("result_state")
    )
    .orderBy(F.col("start_time").desc())
)

display(task_runs)


# COMMAND ----------


# PHASE 6 - STEP 2: TASK PERFORMANCE METRICS

from pyspark.sql import functions as F

task_performance = (
    task_runs
    .groupBy("task_key")
    .agg(
        F.count("*").alias("total_attempts"),

        F.sum(
            F.when(F.col("result_state") == "SUCCEEDED", 1)
             .otherwise(0)
        ).alias("successful_attempts"),

        F.sum(
            F.when(F.col("result_state") == "FAILED", 1)
             .otherwise(0)
        ).alias("failed_attempts"),

        F.sum(
            F.when(F.col("result_state") == "SKIPPED", 1)
             .otherwise(0)
        ).alias("skipped_attempts"),

        F.round(
            F.avg("duration_seconds"), 2
        ).alias("avg_duration_seconds"),

        F.max("duration_seconds").alias(
            "max_duration_seconds"
        ),

        F.min("duration_seconds").alias(
            "min_duration_seconds"
        )
    )
    .withColumn(
        "success_rate_pct",
        F.round(
            F.col("successful_attempts") /
            F.when(
                F.col("total_attempts") -
                F.col("skipped_attempts") > 0,
                F.col("total_attempts") -
                F.col("skipped_attempts")
            ) * 100,
            2
        )
    )
    .orderBy(
        F.col("avg_duration_seconds").desc()
    )
)

display(task_performance)


# COMMAND ----------


# PHASE 8 - DATASET AUDIT MONITORING

from pyspark.sql import functions as F
from pyspark.sql.window import Window

audit = spark.table(
    "ecommerce_dev.monitoring.pipeline_audit"
)

# Latest audit record for each dataset
window = Window.partitionBy(
    "dataset_name", "layer"
).orderBy(
    F.col("logged_at").desc(),
    F.col("audit_id").desc()
)

latest_audit = (
    audit
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

dataset_metrics = latest_audit.select(
    "dataset_name",
    "layer",
    "run_id",
    "records_read",
    "records_written",
    "records_rejected",
    "execution_status",
    "error_message",
    "started_at",
    "completed_at"
).orderBy("dataset_name")

display(dataset_metrics)


# COMMAND ----------


# PHASE 8 - QUARANTINE MONITORING

from pyspark.sql import functions as F

quarantine = spark.table(
    "ecommerce_dev.monitoring.quarantine_records"
)

quarantine_summary = (
    quarantine
    .withColumn(
        "quarantine_reason",
        F.explode_outer("quarantine_reasons")
    )
    .groupBy("dataset_name", "quarantine_reason")
    .agg(
        F.countDistinct("quarantine_id")
        .alias("rejected_records")
    )
    .orderBy(
        F.col("rejected_records").desc()
    )
)

print("TOTAL QUARANTINE RECORDS:", quarantine.count())

print("REJECTED RECORDS BY DATASET AND REASON")
display(quarantine_summary)


# COMMAND ----------


# PHASE 8 - STEP 5: MONITORING HEALTH CHECK
# Read-only: no changes to pipeline data

from pyspark.sql import functions as F

AUDIT_TABLE = "ecommerce_dev.monitoring.pipeline_audit"
QUARANTINE_TABLE = "ecommerce_dev.monitoring.quarantine_records"

audit = spark.table(AUDIT_TABLE)
quarantine = spark.table(QUARANTINE_TABLE)

latest_run = (
    audit.groupBy("run_id")
    .agg(
        F.max("logged_at").alias("last_logged_at")
    )
    .orderBy(F.col("last_logged_at").desc())
    .first()
)

if latest_run is None:
    raise RuntimeError("No audit records available.")

current_audit = audit.filter(
    F.col("run_id") == latest_run["run_id"]
)

summary = current_audit.agg(
    F.countDistinct("dataset_name").alias("datasets_audited"),
    F.sum(
        F.when(F.col("execution_status") == "SUCCESS", 1)
         .otherwise(0)
    ).alias("successful_datasets"),
    F.sum(
        F.when(F.col("execution_status") == "FAILED", 1)
         .otherwise(0)
    ).alias("failed_datasets"),
    F.sum("records_read").alias("records_read"),
    F.sum("records_rejected").alias("records_rejected")
)

print("LATEST AUDIT RUN:", latest_run["run_id"])

print("DATASET PROCESSING SUMMARY")
display(summary)

print("TOTAL QUARANTINE RECORDS:", quarantine.count())

print("LATEST JOB TASK RESULTS")
display(
    task_runs.select(
        "job_run_id",
        "task_key",
        "duration_seconds",
        "result_state"
    ).orderBy(F.col("start_time").desc()).limit(7)
)
