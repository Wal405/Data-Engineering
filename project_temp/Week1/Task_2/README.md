# Week 1 – Task 2: ETL Foundations

Builds a 3-layer ETL pipeline in PostgreSQL: **raw → staging → warehouse**, using customer/product/order/payment data.

## Architecture

## Setup

1. Create the database:
```sql
   CREATE DATABASE week1_task2_db;
   \c week1_task2_db
```
2. Run the scripts **in this exact order**:
```sql
   \i 01_raw_tables.sql
   \i 02_staging_tables.sql
   \i 03_load_staging.sql
   \i 04_warehouse_tables.sql
   \i 05_load_warehouse.sql
   \i 06_analytical_queries.sql
```
3. Import the 4 source CSVs (`Customers.csv`, `Products.csv`, `Orders.csv`, `Payments.csv` — from `Week1/Task_1/data/`) into the `raw` schema tables using DBeaver's Import Data feature, **before** running `03_load_staging.sql`.

## File Guide

| File | Purpose |
|---|---|
| `01_raw_tables.sql` | Creates raw layer tables (loose TEXT typing) |
| `02_staging_tables.sql` | Creates staging layer tables (proper types, PKs) |
| `03_load_staging.sql` | Validates and loads raw → staging (regex checks, casting, TRIM) |
| `04_warehouse_tables.sql` | Creates warehouse star schema (dimensions + fact table, FKs) |
| `05_load_warehouse.sql` | Loads staging → warehouse (dim_customers, dim_products, fact_orders) |
| `06_analytical_queries.sql` | Analytical queries: running total, LAG, LEAD, CTE |

## Key Design Decisions

- **Raw layer uses TEXT for all columns** — prevents failed imports from malformed source data; validation happens deliberately in staging instead.
- **Staging has no foreign keys** — allows inserting/testing cleaned data without FK rejection while still validating.
- **Warehouse enforces foreign keys** — by this layer, data is trusted, so integrity is strictly enforced.
- **fact_orders combines orders + payments** — avoids repeated joins in every analytical query; the join happens once, during the ETL load.
- **All staging/warehouse inserts use `ON CONFLICT DO NOTHING`** — makes the pipeline idempotent (safe to re-run without duplicate errors).

## Row Counts (expected)

| Table | Rows |
|---|---|
| customers | 10 |
| products | 20 |
| orders | 20 |
| payments | 18 |