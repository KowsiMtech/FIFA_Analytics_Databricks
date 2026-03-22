# Databricks notebook source
# MAGIC %run "/FIFA_Sports_Analytics/src/constants"

# COMMAND ----------

# ================================================
# nb_01_bronze.py
# Bronze layer ingestion using Autoloader.
# Processes only new files since last checkpoint.
# No business transformations applied at this layer.
# ================================================

from pyspark.sql import functions as F


# COMMAND ----------

# Autoloader reads new CSV files from the Bronze folder.
# cloudFiles.schemaLocation persists the inferred schema
# to prevent schema evolution errors on subsequent runs.
df_raw = (
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'csv')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .option('cloudFiles.schemaLocation', BRONZE_PATH + '_schema/')
    .load(BRONZE_PATH)
)

# COMMAND ----------

# Metadata columns are added to support lineage tracking
# and FIFA edition identification in downstream layers.
df_raw = (
    df_raw
    .withColumn('source_file',
        F.col('_metadata.file_path'))
    .withColumn('fifa_version',
        F.regexp_extract(
            F.col('_metadata.file_path'),
            r'players_(\d+)\.csv',
            1
        ).cast('integer'))
    .withColumn('ingested_at',
        F.current_timestamp())
)

# COMMAND ----------

# Records are appended to the Bronze Delta table.
# The checkpoint location ensures Autoloader tracks
# which files have been processed across pipeline runs.
(
    df_raw.writeStream
    .format('delta')
    .outputMode('append')
    .option('checkpointLocation', BRONZE_PATH + '_checkpoint/')
    .trigger(availableNow=True)
    .toTable(f'{BRONZE_SCHEMA}.players_raw')
    .awaitTermination()
)

# COMMAND ----------

# Verification: confirms row counts per FIFA edition
df_verify = spark.table(f'{BRONZE_SCHEMA}.players_raw')
print(f'Total Bronze rows ingested: {df_verify.count():,}')
display(
    df_verify
    .groupBy('fifa_version')
    .agg(F.count('*').alias('row_count'))
    .orderBy('fifa_version'))
