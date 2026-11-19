
# src/ecommerce/incremental.py

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def validate_keys(df, keys, label):
    """Prevent ambiguous Delta MERGE operations."""
    if not keys:
        raise ValueError(f"No business keys: {label}")

    if df.groupBy(*keys).count().filter("count > 1").limit(1).count():
        raise ValueError(f"Duplicate business keys: {label}")

    condition = None
    for key in keys:
        missing = F.col(key).isNull()
        condition = missing if condition is None else condition | missing

    if df.filter(condition).limit(1).count():
        raise ValueError(f"Null business keys: {label}")


def load_scd2(spark, df, dataset):
    """Insert new versions and expire changed current versions."""

    table = dataset.full_silver_table
    keys = list(dataset.business_keys)
    tracked = dataset.metadata.get("tracked_columns", [])
    columns = [c["name"] for c in dataset.metadata["columns"]]

    if not tracked:
        raise ValueError(f"Missing tracked columns: {dataset.name}")

    validate_keys(df, keys, dataset.name)

    # First load
    if not spark.catalog.tableExists(table):
        initial = (
            df.withColumn("effective_from", F.current_timestamp())
              .withColumn("effective_to", F.lit(None).cast("timestamp"))
              .withColumn("is_current", F.lit(True))
        )
        initial.write.format("delta").saveAsTable(table)
        return "INITIALIZED_SCD2"

    required = {"effective_from", "effective_to", "is_current"}
    if not required.issubset(set(spark.table(table).columns)):
        raise ValueError(f"SCD2 columns missing in {table}")

    current = spark.table(table).filter(F.col("is_current") == True)
    validate_keys(current, keys, f"current rows in {table}")

    # Detect changes in tracked attributes, including NULL changes
    condition = [
        F.col(f"s.`{k}`") == F.col(f"t.`{k}`")
        for k in keys
    ]

    join_condition = condition[0]
    for part in condition[1:]:
        join_condition = join_condition & part

    compared = df.alias("s").join(
        current.alias("t"),
        join_condition,
        "left"
    )

    changed = None
    for name in tracked:
        difference = ~F.col(f"s.`{name}`").eqNullSafe(
            F.col(f"t.`{name}`")
        )
        changed = difference if changed is None else changed | difference

    new_or_changed = (
        compared.filter(
            F.col(f"t.`{keys[0]}`").isNull() | changed
        )
        .select(
            *[F.col(f"s.`{c}`").alias(c) for c in columns],
            F.col(f"t.`{keys[0]}`").isNotNull().alias("_existing")
        )
    )

    # No changes means no write
    if new_or_changed.limit(1).count() == 0:
        return "NO_CHANGES"

    # Rows that expire existing current versions
    updates = (
        new_or_changed.filter(F.col("_existing"))
        .drop("_existing")
    )

    for key in keys:
        updates = updates.withColumn(
            f"_merge_{key}", F.col(key)
        )

    # Rows inserted as new current versions
    inserts = new_or_changed.drop("_existing")
    for key in keys:
        inserts = inserts.withColumn(
            f"_merge_{key}",
            F.lit(None).cast(df.schema[key].dataType)
        )

    staged = updates.unionByName(inserts)

    staged = (
        staged.withColumn("effective_from", F.current_timestamp())
              .withColumn("effective_to", F.lit(None).cast("timestamp"))
              .withColumn("is_current", F.lit(True))
    )

    match = " AND ".join(
        f"t.`{k}` = s.`_merge_{k}`" for k in keys
    ) + " AND t.is_current = true"

    insert_values = {
        c: f"s.`{c}`"
        for c in columns
    }
    insert_values.update({
        "effective_from": "s.effective_from",
        "effective_to": "s.effective_to",
        "is_current": "s.is_current"
    })

    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(staged.alias("s"), match)
        .whenMatchedUpdate(set={
            "effective_to": "s.effective_from",
            "is_current": "false"
        })
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

    return "SCD2_APPLIED"


def load_silver(spark, df, dataset):
    """Metadata-driven Silver Delta loading."""

    table = dataset.full_silver_table
    strategy = dataset.load_type
    keys = list(dataset.business_keys)
    columns = [c["name"] for c in dataset.metadata["columns"]]

    df = df.select(*columns)

    if strategy == "scd2":
        return load_scd2(spark, df, dataset)

    if strategy == "overwrite_clean":
        cleaned = df.dropDuplicates()

        if not spark.catalog.tableExists(table):
            cleaned.write.format("delta").saveAsTable(table)
            return "CREATED_REFERENCE"

        return "SKIPPED_REFERENCE_REFRESH"

    if strategy not in ("merge", "scd1"):
        raise ValueError(f"Unsupported strategy: {strategy}")

    validate_keys(df, keys, dataset.name)

    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return "CREATED"

    condition = " AND ".join(
        f"t.`{key}` = s.`{key}`" for key in keys
    )

    # Skip unchanged records to reduce unnecessary writes.
    differences = [
        ~F.col(f"t.`{c}`").eqNullSafe(F.col(f"s.`{c}`"))
        for c in columns if c not in keys
    ]

    changed = differences[0] if differences else F.lit(False)
    for expression in differences[1:]:
        changed = changed | expression

    target = DeltaTable.forName(spark, table)

    (
        target.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll(condition=changed)
        .whenNotMatchedInsertAll()
        .execute()
    )

    return "MERGED"
