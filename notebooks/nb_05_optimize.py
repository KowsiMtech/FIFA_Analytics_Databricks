# Databricks notebook source
# nb_05_optimize.py
# Applies OPTIMIZE, ZORDER, and VACUUM to all Delta tables.
# Executed after the Gold and SCD2 tasks complete.

# COMMAND ----------

# MAGIC %run "/FIFA_Sports_Analytics/src/constants"

# COMMAND ----------

# Silver table: ZORDER by overall_rating and market_value_eur
# improves performance for Power BI queries that filter
# or sort by these high-cardinality columns.
spark.sql(f'OPTIMIZE {SILVER_SCHEMA}.players_silver ZORDER BY (overall_rating, market_value_eur)')

# COMMAND ----------

# Gold table optimization targets the columns most
# frequently used in Power BI filters and slicers.
spark.sql(f'OPTIMIZE {GOLD_SCHEMA}.player_rankings    ZORDER BY (overall_rating, fifa_version)')
spark.sql(f'OPTIMIZE {GOLD_SCHEMA}.nation_performance  ZORDER BY (avg_rating, fifa_version)')
spark.sql(f'OPTIMIZE {GOLD_SCHEMA}.club_analysis       ZORDER BY (total_squad_value, fifa_version)')
spark.sql(f'OPTIMIZE {GOLD_SCHEMA}.age_analysis        ZORDER BY (avg_rating, fifa_version)')

# COMMAND ----------

# VACUUM removes files older than 168 hours (7 days).
# Retaining 168 hours preserves time travel capability
# for the preceding seven-day window.
spark.sql(f'VACUUM {SILVER_SCHEMA}.players_silver    RETAIN 168 HOURS')
spark.sql(f'VACUUM {GOLD_SCHEMA}.player_rankings     RETAIN 168 HOURS')
spark.sql(f'VACUUM {GOLD_SCHEMA}.nation_performance  RETAIN 168 HOURS')
spark.sql(f'VACUUM {GOLD_SCHEMA}.club_analysis       RETAIN 168 HOURS')

# COMMAND ----------

# Delta time travel: query the Silver table as it existed
# at version 0 to verify the initial load state.
display(spark.sql(f'SELECT * FROM {SILVER_SCHEMA}.players_silver LIMIT 10'))
print('OPTIMIZE, ZORDER, and VACUUM completed on all Delta tables')


# COMMAND ----------

