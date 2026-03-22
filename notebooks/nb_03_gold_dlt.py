# Databricks notebook source
# ================================================
# nb_03_gold_dlt.py
# Gold layer aggregation tables.
# Each function produces one Gold table.
# Single responsibility: one function, one table.
# ================================================

import dlt
from pyspark.sql import functions as F

# ---------- Constants ----------
CATALOG_NAME  = 'fifa_catalog'
SILVER_SCHEMA = f'{CATALOG_NAME}.silver'
GOLD_SCHEMA   = f'{CATALOG_NAME}.gold'

# COMMAND ----------

# Gold Table 1: Player Rankings
# Provides the full player dataset with all derived columns
# for player-level analysis and ranking in Power BI.
# Gold Table 1: Player Rankings
@dlt.table(
    name = 'fifa_catalog.gold.player_rankings',
    comment = 'All players per FIFA edition with ratings, values, and derived columns.',
    table_properties = {'quality': 'gold'}
)
def player_rankings():
    return spark.readStream.table('players_silver')

# COMMAND ----------

# Gold Table 2: Nation Performance
# Aggregates player counts and rating metrics by nation
# and FIFA edition for nation-level trend analysis.
# Gold Table 2: Nation Performance
@dlt.table(
    name = 'fifa_catalog.gold.nation_performance',
    comment = 'Player count and rating metrics aggregated by nation and FIFA edition.',
    table_properties = {'quality': 'gold'}
)
def nation_performance():
    df = spark.readStream.table('players_silver')
    return (
        df.groupBy('nationality', 'fifa_version')
        .agg(
            F.count(F.lit(1)).alias('player_count'),
            F.avg('overall_rating').alias('avg_rating'),
            F.max('overall_rating').alias('max_rating'),
        )
    )

# COMMAND ----------

# Gold Table 3: Club Analysis
# Aggregates squad value, rating, and wage metrics by club
# and FIFA edition for club investment analysis.
@dlt.table(
    name    = 'fifa_catalog.gold.club_analysis',
    comment = 'Squad value, ratings, and wage metrics aggregated by club and FIFA edition.',
    table_properties = {'quality': 'gold'}
)
def club_analysis():
    return (
        spark.readStream.table('players_silver')
        .groupBy('club_name', 'league_name', 'fifa_version')
        .agg(
            F.count('player_id').alias('squad_size'),
            F.round(F.avg('overall_rating'), 1).alias('avg_squad_rating'),
            F.round(F.sum('market_value_eur'), 0).alias('total_squad_value'),
            F.round(F.sum('weekly_wage_eur'), 0).alias('total_weekly_wages'),
            F.max('overall_rating').alias('best_player_rating'),
            F.round(F.avg('age'), 1).alias('avg_squad_age'),
            F.max('processed_at').alias('last_updated'),
        )
    )


# COMMAND ----------

# Gold Table 4: Age Group Analysis
# Aggregates rating and value metrics by age group
# and FIFA edition for player development analysis.
@dlt.table(
    name    = 'fifa_catalog.gold.age_analysis',
    comment = 'Rating and value metrics aggregated by age group and FIFA edition.',
    table_properties = {'quality': 'gold'}
)
def age_analysis():
    return (
        spark.readStream.table('players_silver')
        .groupBy('age_group', 'fifa_version')
        .agg(
            F.count('player_id').alias('player_count'),
            F.round(F.avg('overall_rating'), 1).alias('avg_rating'),
            F.round(F.avg('potential_rating'), 1).alias('avg_potential'),
            F.round(F.avg('market_value_eur'), 0).alias('avg_value'),
            F.round(F.avg('rating_gap'), 1).alias('avg_rating_gap'),
            F.max('processed_at').alias('last_updated'),
        )
    )