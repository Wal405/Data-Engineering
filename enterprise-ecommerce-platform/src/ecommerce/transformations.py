
from pyspark.sql import functions as F


def prepare_silver(df, dataset):
    """Cast source columns and retain record-level validity flags."""

    projected = []
    invalid_flags = []

    for column in dataset.metadata["columns"]:
        name = column["name"]
        dtype = column["type"]
        required = not column.get("nullable", True)

        raw = F.col(f"`{name}`")
        casted = F.expr(f"try_cast(`{name}` AS {dtype})")

        projected.append(casted.alias(name))

        invalid = raw.isNotNull() & casted.isNull()

        if required:
            invalid = invalid | casted.isNull()

        invalid_flags.append(
            F.coalesce(invalid, F.lit(False))
        )

    # Preserve ingestion metadata for traceability
    audit_cols = [
        F.col(f"`{c}`")
        for c in df.columns
        if c.startswith("_") and c != "_is_valid"
    ]

    valid = ~F.greatest(*invalid_flags)

    return df.select(
        *projected,
        *audit_cols,
        valid.alias("_is_valid")
    )
