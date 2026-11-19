# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- Select project catalog
# MAGIC USE CATALOG ecommerce_dev;
# MAGIC
# MAGIC -- Bronze Layer
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC
# MAGIC -- Silver Layer
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC
# MAGIC -- Gold Layer
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC
# MAGIC -- Configuration
# MAGIC CREATE SCHEMA IF NOT EXISTS config;
# MAGIC
# MAGIC -- Monitoring
# MAGIC CREATE SCHEMA IF NOT EXISTS monitoring;
# MAGIC
# MAGIC -- Verify
# MAGIC SHOW SCHEMAS IN ecommerce_dev;
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- PHASE 1 - STEP 5: CREATE UNITY CATALOG VOLUMES
# MAGIC -- =====================================================
# MAGIC
# MAGIC -- 1. External volume for raw Olist CSV files
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS
# MAGIC ecommerce_dev.bronze.raw_olist_source
# MAGIC LOCATION
# MAGIC 'abfss://source@ecommerceproject1.dfs.core.windows.net/olist/'
# MAGIC COMMENT 'Raw Olist CSV source files';
# MAGIC
# MAGIC
# MAGIC -- 2. Managed volume for Auto Loader state
# MAGIC CREATE VOLUME IF NOT EXISTS
# MAGIC ecommerce_dev.config.streaming_state
# MAGIC COMMENT 'Auto Loader checkpoints and schema tracking';
# MAGIC
# MAGIC
# MAGIC -- 3. Verify volumes
# MAGIC SHOW VOLUMES IN ecommerce_dev.bronze;
# MAGIC
# MAGIC SHOW VOLUMES IN ecommerce_dev.config;
# MAGIC