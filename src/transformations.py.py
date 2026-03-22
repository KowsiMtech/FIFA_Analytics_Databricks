# Databricks notebook source
# MAGIC %run "/FIFA_Sports_Analytics/src/constants"

# COMMAND ----------

# ================================================
# transformations.py
# Reusable transformation functions for the Silver
# pipeline layer. Contains no DLT-specific imports.
# Called by: DLT notebooks and pytest unit tests.
# ================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# COMMAND ----------

# Required columns from Bronze
REQUIRED_COLUMNS = [
    'sofifa_id', 'short_name', 'long_name', 'age',
    'nationality_name', 'club_name', 'league_name',
    'overall', 'potential', 'value_eur', 'wage_eur',
    'player_positions', 'preferred_foot', 
    'release_clause_eur', 'fifa_version',
    'source_file', 'ingested_at'
]

# Column type casts
COLUMN_CASTS = {
    'sofifa_id'  : 'integer',
    'overall'    : 'integer',
    'potential'  : 'integer',
    'age'        : 'integer',
    'value_eur'  : 'double',
    'wage_eur'   : 'double',
    'release_clause_eur' : 'double',
    'fifa_version': 'integer',
}

# COMMAND ----------


def select_required_columns(df: DataFrame) -> DataFrame:
    """Selects only required columns from Bronze DataFrame.
    Columns not in REQUIRED_COLUMNS are excluded from Silver.
    """
    return df.select(*REQUIRED_COLUMNS)

def apply_schema_casts(df: DataFrame) -> DataFrame:
    """Applies explicit type casting using COLUMN_CASTS dictionary.
    Only casts columns present in the DataFrame to handle
    schema variations across FIFA editions.
    """
    for col_name, dtype in COLUMN_CASTS.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))
    return df

def rename_columns(df: DataFrame) -> DataFrame:
    """Renames raw source column names to clean snake_case
    Silver column names as defined in the data dictionary.
    """
    return (
        df
        .withColumnRenamed('sofifa_id',          'player_id')
        .withColumnRenamed('short_name',         'player_name')
        .withColumnRenamed('long_name',          'player_full_name')
        .withColumnRenamed('nationality_name',   'nationality')
        .withColumnRenamed('overall',            'overall_rating')
        .withColumnRenamed('potential',          'potential_rating')
        .withColumnRenamed('value_eur',          'market_value_eur')
        .withColumnRenamed('wage_eur',           'weekly_wage_eur')
        .withColumnRenamed('player_positions',   'positions')
    )

def standardize_strings(df: DataFrame) -> DataFrame:
    """Trims whitespace and standardizes casing on string columns.
    preferred_foot is uppercased to ensure consistent LEFT or RIGHT values.
    """
    return (
        df
        .withColumn('player_name',    F.trim(F.col('player_name')))
        .withColumn('club_name',      F.trim(F.col('club_name')))
        .withColumn('league_name',    F.trim(F.col('league_name')))
        .withColumn('nationality',    F.trim(F.col('nationality')))
        .withColumn('preferred_foot', F.trim(F.upper(F.col('preferred_foot'))))
    )

def handle_nulls(df: DataFrame) -> DataFrame:
    """Replaces null values with domain-appropriate defaults.
    Uses F.when().otherwise() for explicit, readable null handling.
    club_name nulls represent free agents not affiliated with any club.
    Numeric nulls are replaced with 0.0 to preserve aggregation accuracy.
    """
    return (
        df
        .withColumn('club_name',
            F.when(F.col('club_name').isNull(), F.lit('Free Agent'))
             .otherwise(F.col('club_name')))
        .withColumn('league_name',
            F.when(F.col('league_name').isNull(), F.lit('Unknown'))
             .otherwise(F.col('league_name')))
        .withColumn('market_value_eur',
            F.when(F.col('market_value_eur').isNull(), F.lit(0.0))
             .otherwise(F.col('market_value_eur')))
        .withColumn('weekly_wage_eur',
            F.when(F.col('weekly_wage_eur').isNull(), F.lit(0.0))
             .otherwise(F.col('weekly_wage_eur')))
        .withColumn('release_clause_eur',
            F.when(F.col('release_clause_eur').isNull(), F.lit(0.0))
             .otherwise(F.col('release_clause_eur')))
    )

def add_derived_columns(df: DataFrame) -> DataFrame:
    """Adds business-logic derived columns using constants for thresholds.
    value_category classifies players by market valuation tier.
    age_group classifies players by career stage.
    rating_gap measures the difference between potential and current rating.
    """
    return (
        df
        .withColumn('value_category',
            F.when(F.col('market_value_eur') >= VALUE_WORLD_CLASS, F.lit('World Class'))
             .when(F.col('market_value_eur') >= VALUE_ELITE,       F.lit('Elite'))
             .when(F.col('market_value_eur') >= VALUE_GOOD,        F.lit('Good'))
             .when(F.col('market_value_eur') >= VALUE_AVERAGE,     F.lit('Average'))
             .otherwise(F.lit('Budget')))
        .withColumn('age_group',
            F.when(F.col('age') <  AGE_U21,          F.lit('U21 Prospect'))
             .when(F.col('age') <  AGE_YOUNG,        F.lit('Young'))
             .when(F.col('age') <  AGE_PRIME,        F.lit('Prime'))
             .when(F.col('age') <  AGE_EXPERIENCED,  F.lit('Experienced'))
             .otherwise(F.lit('Veteran')))
        .withColumn('rating_gap',
            F.col('potential_rating') - F.col('overall_rating'))
    )

def add_audit_columns(df: DataFrame) -> DataFrame:
    """Adds audit metadata columns for pipeline traceability.
    processed_at captures the Silver processing timestamp and
    serves as the CDC sequence column in Auto CDC Flow.
    silver_version enables tracking of pipeline deployments.
    """
    return (
        df
        .withColumn('processed_at',   F.current_timestamp())
        .withColumn('silver_version',  F.lit('1.0'))
    )

def apply_silver_transforms(df: DataFrame) -> DataFrame:
    """Applies all Silver transformations in the prescribed sequence.
    This is the primary entry point called by the DLT Silver Source
    view and by pytest unit tests. The sequence ensures that column
    renames occur before null handling and derived column creation,
    which depend on the renamed column names.
    """
    df = apply_schema_casts(df)
    df = rename_columns(df)
    df = standardize_strings(df)
    df = handle_nulls(df)
    df = add_derived_columns(df)
    df = add_audit_columns(df)
    return df
