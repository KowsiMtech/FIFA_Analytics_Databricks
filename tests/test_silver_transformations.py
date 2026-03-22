"""
Unit tests for FIFA Sports Analytics – Silver Layer Transformations.

These tests use a local SparkSession (provided by conftest.py / conftest.sql)
and do NOT require a live Databricks cluster or any external storage.

Run with:
    pytest tests/test_silver_transformations.py -v
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
)

# ── Constants (mirrors src/constants.py) ─────────────────────────────────────
# Defined locally so the tests are self-contained and do not rely on
# the Databricks %run magic that loads src/constants at runtime.

REQUIRED_COLUMNS = [
    "sofifa_id", "short_name", "long_name", "age",
    "nationality_name", "club_name", "league_name",
    "overall", "potential", "value_eur", "wage_eur",
    "player_positions", "preferred_foot",
    "release_clause_eur", "fifa_version",
    "source_file", "ingested_at",
]

COLUMN_CASTS = {
    "sofifa_id"         : "integer",
    "overall"           : "integer",
    "potential"         : "integer",
    "age"               : "integer",
    "value_eur"         : "double",
    "wage_eur"          : "double",
    "release_clause_eur": "double",
    "fifa_version"      : "integer",
}

VALUE_WORLD_CLASS = 50_000_000
VALUE_ELITE       = 20_000_000
VALUE_GOOD        =  5_000_000
VALUE_AVERAGE     =  1_000_000

AGE_U21         = 21
AGE_YOUNG       = 25
AGE_PRIME       = 30
AGE_EXPERIENCED = 33

# ── Transformation functions (mirrors src/transformations.py) ─────────────────
# Redefined here so the tests remain runnable in any environment
# (local, CI, or Databricks) without importing Databricks notebooks.

def select_required_columns(df):
    return df.select(*REQUIRED_COLUMNS)


def apply_schema_casts(df):
    for col_name, dtype in COLUMN_CASTS.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))
    return df


def rename_columns(df):
    return (
        df
        .withColumnRenamed("sofifa_id",        "player_id")
        .withColumnRenamed("short_name",        "player_name")
        .withColumnRenamed("long_name",         "player_full_name")
        .withColumnRenamed("nationality_name",  "nationality")
        .withColumnRenamed("overall",           "overall_rating")
        .withColumnRenamed("potential",         "potential_rating")
        .withColumnRenamed("value_eur",         "market_value_eur")
        .withColumnRenamed("wage_eur",          "weekly_wage_eur")
        .withColumnRenamed("player_positions",  "positions")
    )


def standardize_strings(df):
    return (
        df
        .withColumn("player_name",    F.trim(F.col("player_name")))
        .withColumn("club_name",      F.trim(F.col("club_name")))
        .withColumn("league_name",    F.trim(F.col("league_name")))
        .withColumn("nationality",    F.trim(F.col("nationality")))
        .withColumn("preferred_foot", F.trim(F.upper(F.col("preferred_foot"))))
    )


def handle_nulls(df):
    return (
        df
        .withColumn("club_name",
            F.when(F.col("club_name").isNull(), F.lit("Free Agent"))
             .otherwise(F.col("club_name")))
        .withColumn("league_name",
            F.when(F.col("league_name").isNull(), F.lit("Unknown"))
             .otherwise(F.col("league_name")))
        .withColumn("market_value_eur",
            F.when(F.col("market_value_eur").isNull(), F.lit(0.0))
             .otherwise(F.col("market_value_eur")))
        .withColumn("weekly_wage_eur",
            F.when(F.col("weekly_wage_eur").isNull(), F.lit(0.0))
             .otherwise(F.col("weekly_wage_eur")))
        .withColumn("release_clause_eur",
            F.when(F.col("release_clause_eur").isNull(), F.lit(0.0))
             .otherwise(F.col("release_clause_eur")))
    )


def add_derived_columns(df):
    return (
        df
        .withColumn("value_category",
            F.when(F.col("market_value_eur") >= VALUE_WORLD_CLASS, F.lit("World Class"))
             .when(F.col("market_value_eur") >= VALUE_ELITE,        F.lit("Elite"))
             .when(F.col("market_value_eur") >= VALUE_GOOD,         F.lit("Good"))
             .when(F.col("market_value_eur") >= VALUE_AVERAGE,      F.lit("Average"))
             .otherwise(F.lit("Budget")))
        .withColumn("age_group",
            F.when(F.col("age") <  AGE_U21,         F.lit("U21 Prospect"))
             .when(F.col("age") <  AGE_YOUNG,        F.lit("Young"))
             .when(F.col("age") <  AGE_PRIME,        F.lit("Prime"))
             .when(F.col("age") <  AGE_EXPERIENCED,  F.lit("Experienced"))
             .otherwise(F.lit("Veteran")))
        .withColumn("rating_gap",
            F.col("potential_rating") - F.col("overall_rating"))
    )


def add_audit_columns(df):
    return (
        df
        .withColumn("processed_at",   F.current_timestamp())
        .withColumn("silver_version", F.lit("1.0"))
    )


def apply_silver_transforms(df):
    df = apply_schema_casts(df)
    df = rename_columns(df)
    df = standardize_strings(df)
    df = handle_nulls(df)
    df = add_derived_columns(df)
    df = add_audit_columns(df)
    return df


# ═════════════════════════════════════════════════════════════════════════════
# Helper schemas & builders
# ═════════════════════════════════════════════════════════════════════════════

# Full Bronze schema with all REQUIRED_COLUMNS as strings (pre-cast) + extra col
_BRONZE_SCHEMA = StructType([
    StructField("sofifa_id",          StringType(), True),
    StructField("short_name",         StringType(), True),
    StructField("long_name",          StringType(), True),
    StructField("age",                StringType(), True),
    StructField("nationality_name",   StringType(), True),
    StructField("club_name",          StringType(), True),
    StructField("league_name",        StringType(), True),
    StructField("overall",            StringType(), True),
    StructField("potential",          StringType(), True),
    StructField("value_eur",          StringType(), True),
    StructField("wage_eur",           StringType(), True),
    StructField("player_positions",   StringType(), True),
    StructField("preferred_foot",     StringType(), True),
    StructField("release_clause_eur", StringType(), True),
    StructField("fifa_version",       StringType(), True),
    StructField("source_file",        StringType(), True),
    StructField("ingested_at",        StringType(), True),
    StructField("extra_col",          StringType(), True),   # not in REQUIRED_COLUMNS
])

# A realistic Bronze row (string types, messy whitespace, no club/league)
_MESSI_ROW = (
    "158023", " L. Messi ", "Lionel Messi",
    "35", " Argentina ",
    None, None,                              # club & league are null
    "91", "91",
    "55000000.0", "320000.0",
    "CF,RW", " left ",
    "120000000.0", "22",
    "players_22.csv", "2024-01-01",
    "ignore_me",
)

_RONALDO_ROW = (
    "20801", " C. Ronaldo", "Cristiano Ronaldo",
    "37", " Portugal ",
    " Manchester United ", " Premier League ",
    "90", "90",
    "45000000.0", "350000.0",
    "ST,LW", " right ",
    "95000000.0", "22",
    "players_22.csv", "2024-01-01",
    "ignore_me",
)


# ═════════════════════════════════════════════════════════════════════════════
# 1. select_required_columns
# ═════════════════════════════════════════════════════════════════════════════

class TestSelectRequiredColumns:

    def test_returns_exactly_required_columns(self, spark):
        df = spark.createDataFrame([_MESSI_ROW], schema=_BRONZE_SCHEMA)
        result = select_required_columns(df)
        assert set(result.columns) == set(REQUIRED_COLUMNS)

    def test_drops_extra_column(self, spark):
        df = spark.createDataFrame([_MESSI_ROW], schema=_BRONZE_SCHEMA)
        result = select_required_columns(df)
        assert "extra_col" not in result.columns

    def test_row_count_unchanged(self, spark):
        df = spark.createDataFrame([_MESSI_ROW, _RONALDO_ROW], schema=_BRONZE_SCHEMA)
        result = select_required_columns(df)
        assert result.count() == 2

    def test_cell_values_preserved(self, spark):
        df = spark.createDataFrame([_MESSI_ROW], schema=_BRONZE_SCHEMA)
        result = select_required_columns(df)
        row = result.first()
        assert row["sofifa_id"] == "158023"
        assert row["short_name"] == " L. Messi "   # whitespace not yet trimmed


# ═════════════════════════════════════════════════════════════════════════════
# 2. apply_schema_casts
# ═════════════════════════════════════════════════════════════════════════════

class TestApplySchemaCasts:

    _CAST_SCHEMA = StructType([
        StructField("sofifa_id",          StringType(), True),
        StructField("overall",            StringType(), True),
        StructField("potential",          StringType(), True),
        StructField("age",                StringType(), True),
        StructField("value_eur",          StringType(), True),
        StructField("wage_eur",           StringType(), True),
        StructField("release_clause_eur", StringType(), True),
        StructField("fifa_version",       StringType(), True),
    ])

    def test_integer_columns_cast_correctly(self, spark):
        df = spark.createDataFrame(
            [("158023", "91", "91", "35", "55000000.0", "320000.0", "120000000.0", "22")],
            schema=self._CAST_SCHEMA,
        )
        result = apply_schema_casts(df)
        type_map = {f.name: f.dataType.simpleString() for f in result.schema.fields}
        assert type_map["sofifa_id"]   == "int"
        assert type_map["overall"]     == "int"
        assert type_map["potential"]   == "int"
        assert type_map["age"]         == "int"
        assert type_map["fifa_version"] == "int"

    def test_double_columns_cast_correctly(self, spark):
        df = spark.createDataFrame(
            [("158023", "91", "91", "35", "55000000.0", "320000.0", "120000000.0", "22")],
            schema=self._CAST_SCHEMA,
        )
        result = apply_schema_casts(df)
        type_map = {f.name: f.dataType.simpleString() for f in result.schema.fields}
        assert type_map["value_eur"]          == "double"
        assert type_map["wage_eur"]           == "double"
        assert type_map["release_clause_eur"] == "double"

    def test_numeric_values_correct_after_cast(self, spark):
        df = spark.createDataFrame(
            [("158023", "91", "95", "21", "5500000.0", "80000.0", "10000000.0", "20")],
            schema=self._CAST_SCHEMA,
        )
        row = apply_schema_casts(df).first()
        assert row["sofifa_id"]   == 158023
        assert row["overall"]     == 91
        assert row["potential"]   == 95
        assert row["age"]         == 21
        assert row["value_eur"]   == 5_500_000.0
        assert row["fifa_version"] == 20

    def test_missing_columns_skipped_gracefully(self, spark):
        schema = StructType([StructField("sofifa_id", StringType(), True)])
        df = spark.createDataFrame([("99",)], schema=schema)
        result = apply_schema_casts(df)
        assert result.count() == 1          # should not raise

    def test_invalid_string_casts_to_null(self, spark):
        schema = StructType([StructField("age", StringType(), True)])
        df = spark.createDataFrame([("not_a_number",)], schema=schema)
        result = apply_schema_casts(df)
        assert result.first()["age"] is None


# ═════════════════════════════════════════════════════════════════════════════
# 3. rename_columns
# ═════════════════════════════════════════════════════════════════════════════

class TestRenameColumns:

    _RENAME_SCHEMA = StructType([
        StructField("sofifa_id",        IntegerType(), True),
        StructField("short_name",       StringType(),  True),
        StructField("long_name",        StringType(),  True),
        StructField("nationality_name", StringType(),  True),
        StructField("overall",          IntegerType(), True),
        StructField("potential",        IntegerType(), True),
        StructField("value_eur",        DoubleType(),  True),
        StructField("wage_eur",         DoubleType(),  True),
        StructField("player_positions", StringType(),  True),
    ])

    @pytest.fixture(scope="class")
    def renamed_df(self, spark):
        df = spark.createDataFrame(
            [(158023, "L. Messi", "Lionel Messi", "Argentina", 91, 91, 55_000_000.0, 320_000.0, "CF,RW")],
            schema=self._RENAME_SCHEMA,
        )
        return rename_columns(df)

    # new names present
    @pytest.mark.parametrize("new_col", [
        "player_id", "player_name", "player_full_name", "nationality",
        "overall_rating", "potential_rating", "market_value_eur",
        "weekly_wage_eur", "positions",
    ])
    def test_new_column_present(self, renamed_df, new_col):
        assert new_col in renamed_df.columns

    # old names absent
    @pytest.mark.parametrize("old_col", [
        "sofifa_id", "short_name", "long_name", "nationality_name",
        "overall", "potential", "value_eur", "wage_eur", "player_positions",
    ])
    def test_old_column_absent(self, renamed_df, old_col):
        assert old_col not in renamed_df.columns

    def test_values_preserved_after_rename(self, renamed_df):
        row = renamed_df.first()
        assert row["player_id"]         == 158023
        assert row["player_name"]       == "L. Messi"
        assert row["overall_rating"]    == 91
        assert row["market_value_eur"]  == 55_000_000.0
        assert row["positions"]         == "CF,RW"


# ═════════════════════════════════════════════════════════════════════════════
# 4. standardize_strings
# ═════════════════════════════════════════════════════════════════════════════

class TestStandardizeStrings:

    _STD_SCHEMA = StructType([
        StructField("player_name",    StringType(), True),
        StructField("club_name",      StringType(), True),
        StructField("league_name",    StringType(), True),
        StructField("nationality",    StringType(), True),
        StructField("preferred_foot", StringType(), True),
    ])

    @pytest.fixture(scope="class")
    def std_row(self, spark):
        df = spark.createDataFrame(
            [("  L. Messi  ", "  FC Barcelona  ", "  La Liga  ", "  Spain  ", " left ")],
            schema=self._STD_SCHEMA,
        )
        return standardize_strings(df).first()

    def test_player_name_trimmed(self, std_row):
        assert std_row["player_name"] == "L. Messi"

    def test_club_name_trimmed(self, std_row):
        assert std_row["club_name"] == "FC Barcelona"

    def test_league_name_trimmed(self, std_row):
        assert std_row["league_name"] == "La Liga"

    def test_nationality_trimmed(self, std_row):
        assert std_row["nationality"] == "Spain"

    def test_preferred_foot_uppercased_and_trimmed(self, std_row):
        assert std_row["preferred_foot"] == "LEFT"

    def test_right_foot_uppercased(self, spark):
        df = spark.createDataFrame(
            [("CR7", "Man Utd", "Premier League", "Portugal", " right ")],
            schema=self._STD_SCHEMA,
        )
        row = standardize_strings(df).first()
        assert row["preferred_foot"] == "RIGHT"

    def test_already_clean_values_unchanged(self, spark):
        df = spark.createDataFrame(
            [("Neymar", "PSG", "Ligue 1", "Brazil", "RIGHT")],
            schema=self._STD_SCHEMA,
        )
        row = standardize_strings(df).first()
        assert row["player_name"]    == "Neymar"
        assert row["preferred_foot"] == "RIGHT"


# ═════════════════════════════════════════════════════════════════════════════
# 5. handle_nulls
# ═════════════════════════════════════════════════════════════════════════════

class TestHandleNulls:

    _NULL_SCHEMA = StructType([
        StructField("club_name",           StringType(), True),
        StructField("league_name",         StringType(), True),
        StructField("market_value_eur",    DoubleType(),  True),
        StructField("weekly_wage_eur",     DoubleType(),  True),
        StructField("release_clause_eur",  DoubleType(),  True),
    ])

    @pytest.fixture(scope="class")
    def null_row(self, spark):
        df = spark.createDataFrame([(None, None, None, None, None)], schema=self._NULL_SCHEMA)
        return handle_nulls(df).first()

    def test_null_club_becomes_free_agent(self, null_row):
        assert null_row["club_name"] == "Free Agent"

    def test_null_league_becomes_unknown(self, null_row):
        assert null_row["league_name"] == "Unknown"

    def test_null_market_value_becomes_zero(self, null_row):
        assert null_row["market_value_eur"] == 0.0

    def test_null_weekly_wage_becomes_zero(self, null_row):
        assert null_row["weekly_wage_eur"] == 0.0

    def test_null_release_clause_becomes_zero(self, null_row):
        assert null_row["release_clause_eur"] == 0.0

    def test_non_null_values_are_preserved(self, spark):
        df = spark.createDataFrame(
            [("Real Madrid", "La Liga", 100_000_000.0, 500_000.0, 200_000_000.0)],
            schema=self._NULL_SCHEMA,
        )
        row = handle_nulls(df).first()
        assert row["club_name"]          == "Real Madrid"
        assert row["league_name"]        == "La Liga"
        assert row["market_value_eur"]   == 100_000_000.0
        assert row["weekly_wage_eur"]    == 500_000.0
        assert row["release_clause_eur"] == 200_000_000.0


# ═════════════════════════════════════════════════════════════════════════════
# 6. add_derived_columns
# ═════════════════════════════════════════════════════════════════════════════

class TestAddDerivedColumns:

    _DERIVED_SCHEMA = StructType([
        StructField("market_value_eur", DoubleType(),  True),
        StructField("age",              IntegerType(), True),
        StructField("potential_rating", IntegerType(), True),
        StructField("overall_rating",   IntegerType(), True),
    ])

    def _row(self, spark, market_value, age, potential_rating, overall_rating):
        df = spark.createDataFrame(
            [(float(market_value), age, potential_rating, overall_rating)],
            schema=self._DERIVED_SCHEMA,
        )
        return add_derived_columns(df).first()

    # ── value_category ──────────────────────────────────────────────────────

    @pytest.mark.parametrize("value, expected", [
        (50_000_000.0, "World Class"),   # exact boundary
        (55_000_000.0, "World Class"),   # above boundary
        (20_000_000.0, "Elite"),         # exact boundary
        (35_000_000.0, "Elite"),         # between 20M and 50M
        (5_000_000.0,  "Good"),          # exact boundary
        (10_000_000.0, "Good"),          # between 5M and 20M
        (1_000_000.0,  "Average"),       # exact boundary
        (3_000_000.0,  "Average"),       # between 1M and 5M
        (999_999.0,    "Budget"),        # just below Average
        (0.0,          "Budget"),        # zero value
    ])
    def test_value_category_boundaries(self, spark, value, expected):
        row = self._row(spark, value, 25, 85, 80)
        assert row["value_category"] == expected

    # ── age_group ───────────────────────────────────────────────────────────

    @pytest.mark.parametrize("age, expected", [
        (17, "U21 Prospect"),   # well below 21
        (20, "U21 Prospect"),   # just below AGE_U21
        (21, "Young"),          # exactly AGE_U21
        (24, "Young"),          # just below AGE_YOUNG
        (25, "Prime"),          # exactly AGE_YOUNG
        (29, "Prime"),          # just below AGE_PRIME
        (30, "Experienced"),    # exactly AGE_PRIME
        (32, "Experienced"),    # just below AGE_EXPERIENCED
        (33, "Veteran"),        # exactly AGE_EXPERIENCED
        (40, "Veteran"),        # well above threshold
    ])
    def test_age_group_boundaries(self, spark, age, expected):
        row = self._row(spark, 1_000_000.0, age, 80, 78)
        assert row["age_group"] == expected

    # ── rating_gap ──────────────────────────────────────────────────────────

    def test_rating_gap_positive(self, spark):
        row = self._row(spark, 1_000_000.0, 22, 90, 80)
        assert row["rating_gap"] == 10

    def test_rating_gap_zero_when_equal(self, spark):
        row = self._row(spark, 1_000_000.0, 30, 91, 91)
        assert row["rating_gap"] == 0

    def test_rating_gap_negative_on_inconsistent_data(self, spark):
        # Rare but possible if source data has potential < overall
        row = self._row(spark, 1_000_000.0, 28, 85, 87)
        assert row["rating_gap"] == -2


# ═════════════════════════════════════════════════════════════════════════════
# 7. add_audit_columns
# ═════════════════════════════════════════════════════════════════════════════

class TestAddAuditColumns:

    @pytest.fixture(scope="class")
    def audited_row(self, spark):
        schema = StructType([StructField("player_id", IntegerType(), True)])
        df = spark.createDataFrame([(1,)], schema=schema)
        return add_audit_columns(df).first()

    def test_processed_at_column_added(self, audited_row):
        assert "processed_at" in audited_row.asDict()

    def test_silver_version_column_added(self, audited_row):
        assert "silver_version" in audited_row.asDict()

    def test_silver_version_equals_one_point_zero(self, audited_row):
        assert audited_row["silver_version"] == "1.0"

    def test_processed_at_is_not_null(self, audited_row):
        assert audited_row["processed_at"] is not None

    def test_existing_columns_unaffected(self, audited_row):
        assert audited_row["player_id"] == 1


# ═════════════════════════════════════════════════════════════════════════════
# 8. apply_silver_transforms  (integration)
# ═════════════════════════════════════════════════════════════════════════════

class TestApplySilverTransforms:
    """End-to-end test of the full transformation pipeline."""

    # Strip the extra_col field from _BRONZE_SCHEMA for these tests
    _INTEGRATION_SCHEMA = StructType(
        [f for f in _BRONZE_SCHEMA.fields if f.name != "extra_col"]
    )

    _INTEGRATION_DATA = [
        (
            "158023", " L. Messi ",  "Lionel Messi",
            "35", " Argentina ",
            None, None,
            "91", "91",
            "55000000.0", "320000.0",
            "CF,RW", " left ",
            "120000000.0", "22",
            "players_22.csv", "2024-01-01",
        ),
        (
            "20801", " C. Ronaldo", "Cristiano Ronaldo",
            "37", " Portugal ",
            " Manchester United ", " Premier League ",
            "90", "90",
            "45000000.0", "350000.0",
            "ST,LW", " right ",
            "95000000.0", "22",
            "players_22.csv", "2024-01-01",
        ),
    ]

    @pytest.fixture(scope="class")
    def bronze_df(self, spark):
        return spark.createDataFrame(self._INTEGRATION_DATA, schema=self._INTEGRATION_SCHEMA)

    @pytest.fixture(scope="class")
    def silver_df(self, bronze_df):
        return apply_silver_transforms(bronze_df)

    # ── schema ───────────────────────────────────────────────────────────────

    def test_silver_schema_contains_expected_columns(self, silver_df):
        expected = {
            "player_id", "player_name", "player_full_name",
            "age", "nationality", "club_name", "league_name",
            "overall_rating", "potential_rating",
            "market_value_eur", "weekly_wage_eur",
            "positions", "preferred_foot", "release_clause_eur",
            "fifa_version", "source_file", "ingested_at",
            "value_category", "age_group", "rating_gap",
            "processed_at", "silver_version",
        }
        assert expected.issubset(set(silver_df.columns))

    def test_row_count_preserved(self, bronze_df, silver_df):
        assert silver_df.count() == bronze_df.count()

    # ── null handling (Messi row: no club / no league) ────────────────────────

    def test_null_club_filled_with_free_agent(self, silver_df):
        row = silver_df.filter(F.col("player_id") == 158023).first()
        assert row["club_name"] == "Free Agent"

    def test_null_league_filled_with_unknown(self, silver_df):
        row = silver_df.filter(F.col("player_id") == 158023).first()
        assert row["league_name"] == "Unknown"

    # ── string standardisation ───────────────────────────────────────────────

    def test_player_name_trimmed(self, silver_df):
        row = silver_df.filter(F.col("player_id") == 158023).first()
        assert row["player_name"] == "L. Messi"

    def test_preferred_foot_uppercased_messi(self, silver_df):
        row = silver_df.filter(F.col("player_id") == 158023).first()
        assert row["preferred_foot"] == "LEFT"

    def test_preferred_foot_uppercased_ronaldo(self, silver_df):
        row = silver_df.filter(F.col("player_id") == 20801).first()
        assert row["preferred_foot"] == "RIGHT"

    def test_club_name_trimmed_ronaldo(self, silver_df):
        row = silver_df.filter(F.col("player_id") == 20801).first()
        assert row["club_name"] == "Manchester United"

    # ── type casting ─────────────────────────────────────────────────────────

    def test_player_id_is_integer(self, silver_df):
        type_map = {f.name: f.dataType.simpleString() for f in silver_df.schema.fields}
        assert type_map["player_id"] == "int"

    def test_overall_rating_is_integer(self, silver_df):
        type_map = {f.name: f.dataType.simpleString() for f in silver_df.schema.fields}
        assert type_map["overall_rating"] == "int"

    def test_market_value_is_double(self, silver_df):
        type_map = {f.name: f.dataType.simpleString() for f in silver_df.schema.fields}
        assert type_map["market_value_eur"] == "double"

    # ── derived columns ───────────────────────────────────────────────────────

    def test_messi_value_category_world_class(self, silver_df):
        row = silver_df.filter(F.col("player_id") == 158023).first()
        assert row["value_category"] == "World Class"   # 55M >= 50M

    def test_ronaldo_value_category_elite(self, silver_df):
        row = silver_df.filter(F.col("player_id") == 20801).first()
        assert row["value_category"] == "Elite"          # 45M, 20M <= 45M < 50M

    def test_messi_age_group_veteran(self, silver_df):
        row = silver_df.filter(F.col("player_id") == 158023).first()
        assert row["age_group"] == "Veteran"             # age 35 >= 33

    def test_ronaldo_age_group_veteran(self, silver_df):
        row = silver_df.filter(F.col("player_id") == 20801).first()
        assert row["age_group"] == "Veteran"             # age 37 >= 33

    def test_messi_rating_gap_zero(self, silver_df):
        row = silver_df.filter(F.col("player_id") == 158023).first()
        assert row["rating_gap"] == 0                    # 91 potential - 91 overall

    # ── audit columns ─────────────────────────────────────────────────────────

    def test_silver_version_is_one_point_zero(self, silver_df):
        assert silver_df.filter(F.col("silver_version") != "1.0").count() == 0

    def test_processed_at_not_null_for_all_rows(self, silver_df):
        assert silver_df.filter(F.col("processed_at").isNull()).count() == 0
