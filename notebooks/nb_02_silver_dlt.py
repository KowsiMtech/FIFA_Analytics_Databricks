# Databricks notebook source
# ================================================
# nb_02_silver_dlt.py
# Silver DLT pipeline - three-step architecture
# ================================================

import dlt
from pyspark.sql import functions as F

# ---------- Constants ----------
CATALOG_NAME        = 'fifa_catalog'
BRONZE_SCHEMA       = f'{CATALOG_NAME}.bronze'
SILVER_SCHEMA       = f'{CATALOG_NAME}.silver'
SILVER_KEY_COLUMNS  = ['player_id', 'fifa_version']

MIN_OVERALL_RATING  = 1
MAX_OVERALL_RATING  = 99
MIN_AGE             = 15
MAX_AGE             = 50
MIN_FIFA_VERSION    = 15
MAX_FIFA_VERSION    = 22

VALUE_WORLD_CLASS   = 50_000_000
VALUE_ELITE         = 20_000_000
VALUE_GOOD          =  5_000_000
VALUE_AVERAGE       =  1_000_000

AGE_U21             = 21
AGE_YOUNG           = 25
AGE_PRIME           = 30
AGE_EXPERIENCED     = 33

REQUIRED_COLUMNS = [
    'sofifa_id', 'short_name', 'long_name', 'age',
    'height_cm', 'weight_kg', 'nationality',
    'club', 'league_name', 'overall', 'potential',
    'value_eur', 'wage_eur', 'player_positions', 'preferred_foot',
    'international_reputation', 'release_clause_eur',
    'fifa_version', 'source_file', 'ingested_at'
]

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

# ---------- Transformation Functions ----------
def select_required_columns(df):
    available = [c for c in REQUIRED_COLUMNS if c in df.columns]
    return df.select(*available)

def apply_schema_casts(df):
    for col_name, dtype in COLUMN_CASTS.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))
    return df

def rename_columns(df):
    if 'club' in df.columns and 'club_name' not in df.columns:
        df = df.withColumnRenamed('club', 'club_name')
    if 'nationality_name' in df.columns:
        df = df.withColumnRenamed('nationality_name', 'nationality')
    return (
        df
        .withColumnRenamed('sofifa_id',          'player_id')
        .withColumnRenamed('short_name',         'player_name')
        .withColumnRenamed('long_name',          'player_full_name')
        .withColumnRenamed('nationality_name',   'nationality')
        .withColumnRenamed('club',               'club_name')
        .withColumnRenamed('overall',            'overall_rating')
        .withColumnRenamed('potential',          'potential_rating')
        .withColumnRenamed('value_eur',          'market_value_eur')
        .withColumnRenamed('wage_eur',           'weekly_wage_eur')
        .withColumnRenamed('player_positions',   'positions')
    )



def standardize_strings(df):
    if 'player_name' in df.columns:
        df = df.withColumn('player_name', F.trim(F.col('player_name')))
    if 'club_name' in df.columns:
        df = df.withColumn('club_name', F.trim(F.col('club_name')))
    if 'league_name' in df.columns:
        df = df.withColumn('league_name', F.trim(F.col('league_name')))
    if 'nationality' in df.columns:
        df = df.withColumn('nationality', F.trim(F.col('nationality')))
    if 'preferred_foot' in df.columns:
        df = df.withColumn('preferred_foot', F.trim(F.upper(F.col('preferred_foot'))))
    return df

def handle_nulls(df):
    if 'club_name' in df.columns:
        df = df.withColumn('club_name',
            F.when(F.col('club_name').isNull(), F.lit('Free Agent'))
             .otherwise(F.col('club_name')))
    if 'league_name' in df.columns:
        df = df.withColumn('league_name',
            F.when(F.col('league_name').isNull(), F.lit('Unknown'))
             .otherwise(F.col('league_name')))
    if 'market_value_eur' in df.columns:
        df = df.withColumn('market_value_eur',
            F.when(F.col('market_value_eur').isNull(), F.lit(0.0))
             .otherwise(F.col('market_value_eur')))
    if 'weekly_wage_eur' in df.columns:
        df = df.withColumn('weekly_wage_eur',
            F.when(F.col('weekly_wage_eur').isNull(), F.lit(0.0))
             .otherwise(F.col('weekly_wage_eur')))
    if 'release_clause_eur' in df.columns:
        df = df.withColumn('release_clause_eur',
            F.when(F.col('release_clause_eur').isNull(), F.lit(0.0))
             .otherwise(F.col('release_clause_eur')))
    return df
def add_derived_columns(df):
    return (
        df
        .withColumn('value_category',
            F.when(F.col('market_value_eur') >= VALUE_WORLD_CLASS, F.lit('World Class'))
             .when(F.col('market_value_eur') >= VALUE_ELITE,       F.lit('Elite'))
             .when(F.col('market_value_eur') >= VALUE_GOOD,        F.lit('Good'))
             .when(F.col('market_value_eur') >= VALUE_AVERAGE,     F.lit('Average'))
             .otherwise(F.lit('Budget')))
        .withColumn('age_group',
            F.when(F.col('age') < AGE_U21,         F.lit('U21 Prospect'))
             .when(F.col('age') < AGE_YOUNG,       F.lit('Young'))
             .when(F.col('age') < AGE_PRIME,       F.lit('Prime'))
             .when(F.col('age') < AGE_EXPERIENCED, F.lit('Experienced'))
             .otherwise(F.lit('Veteran')))
        .withColumn('rating_gap',
            F.col('potential_rating') - F.col('overall_rating'))
    )

def add_audit_columns(df):
    return (
        df
        .withColumn('processed_at',   F.current_timestamp())
        .withColumn('silver_version',  F.lit('1.0'))
    )

def apply_silver_transforms(df):
    df = select_required_columns(df)   # ← Step 0: Select columns first!
    df = apply_schema_casts(df)
    df = rename_columns(df)
    df = standardize_strings(df)
    df = handle_nulls(df)
    df = add_derived_columns(df)
    df = add_audit_columns(df)
    return df

# ---------- Step 1: Silver Source View ----------
# should read from Bronze!
@dlt.view(name='players_silver_source')
def players_silver_source():
    df = spark.readStream.table(f'{BRONZE_SCHEMA}.players_raw')  # ← Fix this
    return apply_silver_transforms(df)

# COMMAND ----------

# Step 2: Silver Validated View enforces data quality.
# expect_or_drop excludes records failing the expectation.
# expect logs violations but retains the record.
# Expectation thresholds reference constants to match
# the same values used in transformation logic.

@dlt.expect_or_drop('valid_player_id',
    'player_id IS NOT NULL')

@dlt.expect_or_drop('valid_overall_rating',
    f'overall_rating BETWEEN {MIN_OVERALL_RATING} AND {MAX_OVERALL_RATING}')

@dlt.expect_or_drop('valid_age',
    f'age BETWEEN {MIN_AGE} AND {MAX_AGE}')

@dlt.expect_or_drop('valid_player_name',
    'player_name IS NOT NULL AND LENGTH(player_name) > 0')

@dlt.expect_or_drop('valid_fifa_version',
    f'fifa_version BETWEEN {MIN_FIFA_VERSION} AND {MAX_FIFA_VERSION}')

# Warning-level expectations: violations are logged but
# records are retained because these values may legitimately
# be zero for free agents or youth players.
@dlt.expect('non_negative_market_value', 'market_value_eur >= 0')
@dlt.expect('non_negative_wage',         'weekly_wage_eur >= 0')
@dlt.expect('non_negative_rating_gap',   'rating_gap >= 0')

@dlt.view(name='players_silver_validated')
def players_silver_validated():
    return spark.readStream.table('players_silver_source')


# COMMAND ----------

'''# Step 3: Silver final table
@dlt.table(
    name='players_silver',
    comment='Cleaned and validated FIFA player data.',
    table_properties={
        'quality': 'silver',
        'pipelines.autoOptimize.managed': 'true',
    }
)
def players_silver():
    return spark.readStream.table('players_silver_validated')'''

# COMMAND ----------

# Step 3: Silver final table with CDC upsert (SCD Type 1)
dlt.create_streaming_table(
    name='players_silver',
    comment='Cleaned and validated FIFA player data with CDC upsert logic.',
    table_properties={
        'quality': 'silver',
        'pipelines.autoOptimize.managed': 'true',
    }
)

dlt.apply_changes(
    target             = 'players_silver',
    source             = 'players_silver_validated',
    keys               = SILVER_KEY_COLUMNS,
    sequence_by        = F.col('processed_at'),
    stored_as_scd_type = 1
)
