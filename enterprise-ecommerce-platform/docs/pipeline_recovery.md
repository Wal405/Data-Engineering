# Pipeline Failure Recovery Runbook

**Project:** Enterprise E-Commerce Data Platform  
**Environment:** Development (DEV)  
**Lakeflow Job:** `enterprise_ecommerce_dev_pipeline`  
**Catalog:** `ecommerce_dev`

## 1. Detect a Pipeline Failure

When a failure notification is received:

1. Open Azure Databricks → Jobs & Pipelines.
2. Select `enterprise_ecommerce_dev_pipeline`.
3. Open the failed execution from the Runs tab.
4. Identify the first failed task and read its error message.
5. Record the Job run ID, failed task, failure time, and error details.

## 2. Investigate the Failure

Check the relevant source:

| Failure type | Investigation |
|---|---|
| Bronze ingestion | Source files, storage permissions, Auto Loader checkpoints |
| Silver transformations | Schema, data-quality rules, Delta MERGE operations |
| Silver validation | Foreign-key integrity, source-to-target reconciliation |
| Gold dimensions | SCD Type 2 changes, business keys, Delta operations |
| Gold facts | Dimension dependencies, join conditions, fact-table grain |
| Gold validation | Row-count reconciliation, duplicate keys, referential integrity |
| Audit logging | `ecommerce_dev.monitoring.pipeline_audit` |

Do not delete checkpoints, truncate Delta tables, or overwrite source data as a troubleshooting shortcut.

## 3. Repair the Failed Job

After resolving the underlying error:

1. Open the failed job run.
2. Select **Repair run**.
3. Review which failed or skipped tasks will execute.
4. Confirm those tasks can safely be rerun without duplicate or inconsistent data.
5. Start the repair.
6. Review the repaired run's outcome.

Repairing a run does not automatically make transformations idempotent. A task that partially committed data before failing may write it again.

## 4. Verify Data After Recovery

Check that:

- All required tasks succeeded.
- Bronze and Silver reconciliation checks pass.
- Gold validation checks pass.
- SCD Type 2 tables have the expected current and historical rows.
- Quarantine records are preserved.
- Audit records correctly describe execution status and rejected counts.
- No unexpected duplicate records were introduced.

## 5. Record the Incident

Document:

- Job run ID and task
- Failure reason
- Corrective action
- Whether a repair or full rerun was performed
- Data validation outcome
- Follow-up action to prevent recurrence

## 6. Escalation

If the underlying error is unknown, or data may be inconsistent after a partial write, do not repeatedly rerun the job. Investigate the table transaction history and processing logic before continuing.
