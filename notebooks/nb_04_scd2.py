# Databricks notebook source
# MAGIC %run "/FIFA_Sports_Analytics/src/constants"

# COMMAND ----------

# ================================================
# nb_04_scd2.py
# SCD Type 2 implementation for player transfer history.
# Tracks club changes across all six FIFA editions.
# Produces gold.transfer_history_scd2 table.
# ================================================

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# Read player and club data from Silver layer.
# Only columns required for SCD2 are selected.
df = (
    spark.table(f'{SILVER_SCHEMA}.players_silver')
    .select(
        'player_id', 'player_name', 'nationality',
        'club_name', 'league_name', 'overall_rating',
        'market_value_eur', 'fifa_version'
    )
)

# COMMAND ----------

# Window partitioned by player to compare editions.
window_spec = Window.partitionBy('player_id').orderBy('fifa_version')

# COMMAND ----------

df_with_changes = (
    df
    # Compare each record's club against the previous edition.
    .withColumn('prev_club',
        F.lag('club_name', 1).over(window_spec))
    # Flag records where the club has changed or where
    # no previous record exists (first appearance).
    .withColumn('club_changed',
        F.when(F.col('prev_club').isNull(), F.lit(True))
         .when(F.col('club_name') != F.col('prev_club'), F.lit(True))
         .otherwise(F.lit(False)))
    # Identify whether a subsequent edition record exists.
    .withColumn('next_version',
        F.lead('fifa_version', 1).over(window_spec))
    # is_current is True only when no next record exists.
    .withColumn('is_current',
        F.when(F.col('next_version').isNull(), F.lit(True))
         .otherwise(F.lit(False)))
    # Convert FIFA edition numbers to calendar years.
    .withColumn('start_year', F.col('fifa_version') + 2000)
    .withColumn('end_year',
        F.when(F.col('is_current'), F.lit(None))
         .otherwise(F.col('next_version') + 2000))
)

# COMMAND ----------

# Retain only records representing a club change.
# This filters out editions where the player remained
# at the same club, keeping only transition records.
df_scd2 = (
    df_with_changes
    .filter(F.col('club_changed') == True)
    .select(
        'player_id', 'player_name', 'nationality',
        'club_name', 'league_name', 'overall_rating',
        'market_value_eur', 'fifa_version',
        'start_year', 'end_year', 'is_current'
    )
)

# COMMAND ----------

df_scd2.write.format('delta').mode('overwrite') \
    .saveAsTable(f'{GOLD_SCHEMA}.transfer_history_scd2')

# COMMAND ----------

df_scd2.write.format('parquet').mode('overwrite') \
    .save('abfss://fifa-data@fifastoragectl.dfs.core.windows.net/gold/transfer_history_scd2/')

# COMMAND ----------

total   = spark.table(f'{GOLD_SCHEMA}.transfer_history_scd2').count()
current = spark.table(f'{GOLD_SCHEMA}.transfer_history_scd2').filter('is_current = true').count()
print(f'SCD2 complete: {total:,} total records, {current:,} currently active records')

# COMMAND ----------

