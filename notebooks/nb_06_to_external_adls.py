# Databricks notebook source
# Paths
ADLS = 'abfss://fifa-data@fifastoragectl.dfs.core.windows.net'

tables = [
    ('fifa_catalog.silver', 'players_silver',   f'{ADLS}/silver/players_silver'),
    ('fifa_catalog.gold',   'player_rankings',  f'{ADLS}/gold/player_rankings'),
    ('fifa_catalog.gold',   'nation_performance',f'{ADLS}/gold/nation_performance'),
    ('fifa_catalog.gold',   'club_analysis',    f'{ADLS}/gold/club_analysis'),
    ('fifa_catalog.gold',   'age_analysis',     f'{ADLS}/gold/age_analysis'),
]

for schema, table, path in tables:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {schema}.{table}_external
        LOCATION '{path}'
        AS SELECT * FROM {schema}.{table}
    """)
    print(f"Done: {schema}.{table}_external → {path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED fifa_catalog.silver.players_silver

# COMMAND ----------

