"""Enterprise e-commerce Gold models: validated, idempotent Delta upserts.

Source: ecommerce_dev.silver; target: ecommerce_dev.gold.
No writes to Bronze or Silver. Snapshot-oriented; not an event-time CDC processor.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def _name(catalog, schema, table):
    return f"`{catalog}`.`{schema}`.`{table}`"


def _location(dataset):
    p = dataset.config["platform"]
    return p["catalog"], p["schemas"]["silver"], p["schemas"]["gold"]


def _assert_keys(df, keys, label):
    if not keys or any(k not in df.columns for k in keys):
        raise ValueError(f"Missing key column(s) in {label}: {keys}")
    nulls = None
    for key in keys:
        check = F.col(key).isNull()
        nulls = check if nulls is None else nulls | check
    if df.filter(nulls).limit(1).count():
        raise ValueError(f"Null key(s) in {label}: {keys}")
    if df.groupBy(*keys).count().filter("count > 1").limit(1).count():
        raise ValueError(f"Duplicate key(s) in {label}: {keys}")


def _assert_resolved(df, key_columns, label):
    for name in key_columns:
        if df.filter(F.col(name).isNull()).limit(1).count():
            raise ValueError(f"Unresolved dimension key {name} in {label}")


def _upsert(spark, df, target, keys):
    """Insert first snapshot or MERGE changed records; never delete target rows."""
    _assert_keys(df, keys, target)
    if not spark.catalog.tableExists(target):
        (df.withColumn("_gold_created_at", F.current_timestamp())
           .withColumn("_gold_updated_at", F.current_timestamp())
           .write.format("delta").saveAsTable(target))
        return "CREATED"

    existing = spark.table(target)
    required = set(df.columns) | {"_gold_created_at", "_gold_updated_at"}
    if not required.issubset(set(existing.columns)):
        raise ValueError(f"Gold schema mismatch in {target}; migrate explicitly")
    _assert_keys(existing, keys, f"existing {target}")
    data_columns = df.columns
    value_columns = [c for c in data_columns if c not in keys]
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in value_columns)
    merge = DeltaTable.forName(spark, target).alias("t").merge(df.alias("s"), on)
    if changed:
        update = {c: f"s.`{c}`" for c in value_columns}
        update["_gold_updated_at"] = "current_timestamp()"
        merge = merge.whenMatchedUpdate(condition=changed, set=update)
    insert = {c: f"s.`{c}`" for c in data_columns}
    insert.update({"_gold_created_at": "current_timestamp()", "_gold_updated_at": "current_timestamp()"})
    merge.whenNotMatchedInsert(values=insert).execute()
    return "MERGED"


def _current_dimension(spark, table, business_key, alias):
    df = spark.table(table).filter(F.col("is_current") == True)
    _assert_keys(df, [business_key], f"current {table}")
    return df.select(business_key, F.col("dimension_key").alias(alias))


def build_dimension(spark, dataset, enrich_geography=True):
    """Sync one SCD2 dimension, preserving every Silver historical version."""
    if dataset.load_type != "scd2":
        raise ValueError(f"Dimension must be SCD2: {dataset.name}")
    keys = list(dataset.business_keys)
    if len(keys) != 1:
        raise ValueError(f"Expected one dimension business key: {dataset.name}")
    key = keys[0]
    catalog, silver_schema, gold_schema = _location(dataset)
    target = _name(catalog, gold_schema, f"dim_{dataset.name}")
    df = spark.table(dataset.full_silver_table)
    history = {"effective_from", "effective_to", "is_current"}
    if not history.issubset(df.columns):
        raise ValueError(f"Missing SCD2 history columns: {dataset.name}")
    if df.filter(F.col("effective_from").isNull() | F.col("is_current").isNull()).limit(1).count():
        raise ValueError(f"Invalid SCD2 timestamps/flags: {dataset.name}")
    if df.filter((F.col("is_current") == True) & F.col("effective_to").isNotNull()).limit(1).count():
        raise ValueError(f"Current row has effective_to: {dataset.name}")
    if df.filter((F.col("is_current") == False) & F.col("effective_to").isNull()).limit(1).count():
        raise ValueError(f"Expired row missing effective_to: {dataset.name}")
    if df.filter(F.col("effective_to").isNotNull() & (F.col("effective_to") < F.col("effective_from"))).limit(1).count():
        raise ValueError(f"Reversed SCD2 interval: {dataset.name}")
    _assert_keys(df.filter(F.col("is_current") == True), keys, f"current {dataset.name}")
    _assert_keys(df, [key, "effective_from"], f"SCD2 versions {dataset.name}")

    if enrich_geography and dataset.name in ("customers", "sellers"):
        zip_col = f"{dataset.name[:-1]}_zip_code_prefix"  # customer/seller
        geo_name = _name(catalog, silver_schema, "silver_geolocation")
        if zip_col not in df.columns:
            raise ValueError(f"Missing ZIP column: {zip_col}")
        if not spark.catalog.tableExists(geo_name):
            raise ValueError(f"Missing geolocation table: {geo_name}")
        geo = (spark.table(geo_name)
               .groupBy("geolocation_zip_code_prefix")
               .agg(F.round(F.avg("geolocation_lat"), 6).alias("latitude"),
                    F.round(F.avg("geolocation_lng"), 6).alias("longitude")))
        df = (df.join(geo, df[zip_col] == geo["geolocation_zip_code_prefix"], "left")
              .drop("geolocation_zip_code_prefix"))

    df = df.withColumn(
        "dimension_key", F.sha2(F.to_json(F.struct(F.col(key), F.col("effective_from"))), 256)
    )
    return _upsert(spark, df, target, ["dimension_key"])


def build_dim_date(spark, catalog, silver_schema, gold_schema):
    """Create/extend a date dimension covering observed order/delivery dates."""
    orders = spark.table(_name(catalog, silver_schema, "silver_orders"))
    date_cols = ["order_purchase_timestamp", "order_approved_at",
                 "order_delivered_carrier_date", "order_delivered_customer_date",
                 "order_estimated_delivery_date"]
    dates = orders.select(F.explode(F.array(*[F.to_date(c) for c in date_cols])).alias("full_date"))
    bounds = dates.agg(F.min("full_date").alias("start"), F.max("full_date").alias("end")).first()
    if bounds["start"] is None or bounds["end"] is None:
        raise ValueError("Cannot determine date dimension range")
    # Pad the calendar to support adjacent business/reporting periods.
    start = bounds["start"].replace(month=1, day=1)
    end = bounds["end"].replace(month=12, day=31)
    calendar = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS full_date"
    )
    df = (calendar.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
          .withColumn("year", F.year("full_date"))
          .withColumn("quarter", F.quarter("full_date"))
          .withColumn("month", F.month("full_date"))
          .withColumn("month_name", F.date_format("full_date", "MMMM"))
          .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
          .withColumn("day_of_month", F.dayofmonth("full_date"))
          .withColumn("day_of_week", F.dayofweek("full_date"))
          .withColumn("day_name", F.date_format("full_date", "EEEE"))
          .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7)))
    return _upsert(spark, df, _name(catalog, gold_schema, "dim_date"), ["date_key"])


def build_fact_orders(spark, catalog, silver_schema, gold_schema):
    """One row per order. Child measures are pre-aggregated to avoid fan-out."""
    s = lambda name: _name(catalog, silver_schema, f"silver_{name}")
    g = lambda name: _name(catalog, gold_schema, name)
    orders = spark.table(s("orders"))
    _assert_keys(orders, ["order_id"], "silver_orders")
    customers = _current_dimension(spark, g("dim_customers"), "customer_id", "customer_dimension_key")
    items = (spark.table(s("order_items")).groupBy("order_id")
             .agg(F.count("order_item_id").alias("item_count"),
                  F.sum("price").cast("decimal(18,2)").alias("items_subtotal"),
                  F.sum("freight_value").cast("decimal(18,2)").alias("freight_total")))
    payments = (spark.table(s("order_payments")).groupBy("order_id")
                .agg(F.sum("payment_value").cast("decimal(18,2)").alias("total_paid_value"),
                     F.max("payment_installments").alias("max_installments")))
    reviews = (spark.table(s("order_reviews")).groupBy("order_id")
               .agg(F.round(F.avg("review_score"), 2).alias("avg_review_score")))
    df = (orders.join(customers, "customer_id", "left")
          .join(items, "order_id", "left")
          .join(payments, "order_id", "left")
          .join(reviews, "order_id", "left")
          .withColumn("gross_order_value", (F.col("items_subtotal") + F.col("freight_total")).cast("decimal(18,2)"))
          .withColumn("order_date_key", F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int"))
          .withColumn("actual_delivery_days", F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
          .withColumn("delivery_delay_days", F.datediff("order_delivered_customer_date", "order_estimated_delivery_date"))
          .withColumn("is_delivered_on_time", F.when(
              F.col("order_delivered_customer_date").isNotNull() &
              F.col("order_estimated_delivery_date").isNotNull(),
              F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"))))
    _assert_keys(df, ["order_id"], "fact_orders")
    _assert_resolved(df, ["customer_dimension_key"], "fact_orders")
    return _upsert(spark, df, g("fact_orders"), ["order_id"])


def build_fact_order_items(spark, catalog, silver_schema, gold_schema):
    """One row per order_id + order_item_id; uses current dimension snapshot."""
    s = lambda name: _name(catalog, silver_schema, f"silver_{name}")
    g = lambda name: _name(catalog, gold_schema, name)
    items = spark.table(s("order_items"))
    _assert_keys(items, ["order_id", "order_item_id"], "silver_order_items")
    orders = spark.table(g("fact_orders")).select(
        "order_id", "customer_dimension_key", "order_date_key", "order_status", "is_delivered_on_time")
    products = _current_dimension(spark, g("dim_products"), "product_id", "product_dimension_key")
    sellers = _current_dimension(spark, g("dim_sellers"), "seller_id", "seller_dimension_key")
    df = (items.join(orders, "order_id", "left")
          .join(products, "product_id", "left")
          .join(sellers, "seller_id", "left")
          .withColumn("total_item_value", (F.col("price") + F.col("freight_value")).cast("decimal(18,2)"))
          .select("order_id", "order_item_id", "product_id", "seller_id",
                  "customer_dimension_key", "product_dimension_key", "seller_dimension_key",
                  "order_date_key", "order_status", "shipping_limit_date", "price",
                  "freight_value", "total_item_value", "is_delivered_on_time"))
    _assert_keys(df, ["order_id", "order_item_id"], "fact_order_items")
    _assert_resolved(df, ["customer_dimension_key", "product_dimension_key", "seller_dimension_key"], "fact_order_items")
    return _upsert(spark, df, g("fact_order_items"), ["order_id", "order_item_id"])


def build_fact_payments(spark, catalog, silver_schema, gold_schema):
    """One row per order_id + payment_sequential; never join to item detail."""
    s = lambda name: _name(catalog, silver_schema, f"silver_{name}")
    g = lambda name: _name(catalog, gold_schema, name)
    payments = spark.table(s("order_payments"))
    _assert_keys(payments, ["order_id", "payment_sequential"], "silver_order_payments")
    orders = spark.table(g("fact_orders")).select("order_id", "customer_dimension_key", "order_date_key")
    df = payments.join(orders, "order_id", "left")
    _assert_keys(df, ["order_id", "payment_sequential"], "fact_payments")
    _assert_resolved(df, ["customer_dimension_key", "order_date_key"], "fact_payments")
    return _upsert(spark, df, g("fact_payments"), ["order_id", "payment_sequential"])
