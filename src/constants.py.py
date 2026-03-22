# Databricks notebook source
# ================================================
# Centralized configuration for the FIFA
# Sports Analytics pipeline. All pipeline constants,
# paths, schema definitions and business rules are
# defined here. No hardcoded values appear in notebooks.
# ================================================

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG fifa_catalog

# COMMAND ----------

# Azure Storage Configuration 
STORAGE_ACCOUNT = 'fifastoragectl'
CONTAINER_NAME  = 'fifa-data'
CATALOG_NAME    = 'fifa_catalog'

BRONZE_PATH = f'abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/'
SILVER_PATH = f'abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/'
GOLD_PATH   = f'abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/'


# COMMAND ----------

#  Unity Catalog Schema References 
BRONZE_SCHEMA = f'{CATALOG_NAME}.bronze'
SILVER_SCHEMA = f'{CATALOG_NAME}.silver'
GOLD_SCHEMA   = f'{CATALOG_NAME}.gold'


# COMMAND ----------

# CDC Key Columns
# Composite key identifying a unique record in Silver
# player_id alone is insufficient — same player appears
# in multiple FIFA editions with different attributes
SILVER_KEY_COLUMNS = ['player_id', 'fifa_version']

# COMMAND ----------

# Schema Casting Dictionary
# Explicit type casting prevents schema drift across
# pipeline runs. When a column type changes, update
# this dictionary rather than individual notebooks.
COLUMN_CASTS = {
    'sofifa_id'                : 'integer',
    'age'                      : 'integer',
    'height_cm'                : 'integer',
    'weight_kg'                : 'integer',
    'overall'                  : 'integer',
    'potential'                : 'integer',
    'value_eur'                : 'double',
    'wage_eur'                 : 'double',
    'release_clause_eur'       : 'double',
    'international_reputation' : 'integer',
}

# COMMAND ----------

# Required Column Selection
# Silver selects only these columns from Bronze.
# Columns absent from this list are excluded from
# the Silver layer, reducing storage and query cost.
REQUIRED_COLUMNS = [
    'sofifa_id', 'short_name', 'long_name', 'age',
    'height_cm', 'weight_kg', 'nationality_name', 'club_name',
    'league_name', 'overall', 'potential', 'value_eur', 'wage_eur',
    'player_positions', 'preferred_foot', 'international_reputation',
    'release_clause_eur', 'fifa_version',
]

# COMMAND ----------

# Market Value Classification Thresholds
# Named constants document business rule intent.
# Changing a threshold requires updating one line here.
VALUE_WORLD_CLASS = 50_000_000   # market_value_eur >= 50M EUR
VALUE_ELITE       = 20_000_000   # market_value_eur >= 20M EUR
VALUE_GOOD        =  5_000_000   # market_value_eur >= 5M EUR
VALUE_AVERAGE     =  1_000_000   # market_value_eur >= 1M EUR
# market_value_eur < 1M EUR is classified as Budget

# Age Group Classification Thresholds
AGE_U21           = 21           # age < 21 = U21 Prospect
AGE_YOUNG         = 25           # age < 25 = Young
AGE_PRIME         = 30           # age < 30 = Prime
AGE_EXPERIENCED   = 33           # age < 33 = Experienced
# age >= 33 is classified as Veteran

# Data Quality Thresholds
MIN_OVERALL_RATING = 1
MAX_OVERALL_RATING = 99
MIN_AGE            = 15
MAX_AGE            = 50
MIN_FIFA_VERSION   = 15
MAX_FIFA_VERSION   = 22