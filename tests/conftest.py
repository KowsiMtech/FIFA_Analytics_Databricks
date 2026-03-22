import os
import sys
import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# ── Python 3.14 + Windows + PySpark workaround ───────────────────────────────
# PySpark's Python worker process (spawned for each Spark task) uses stdin/stdout
# for JVM↔Python serialization. On Python 3.14 + Windows this communication
# crashes silently (java.io.EOFException / "Python worker exited unexpectedly").
#
# Fix: route all createDataFrame(list, StructType) calls through pandas/Arrow,
# which serializes data on the driver via the Arrow C extension — no worker
# process spawned, no socket I/O, fully compatible with Python 3.14.
# ─────────────────────────────────────────────────────────────────────────────

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

_orig_create_df = SparkSession.createDataFrame


def _arrow_create_df(self, data, schema=None, samplingRatio=None, verifySchema=True):
    """Intercepts list+StructType calls and converts them via pandas/Arrow."""
    if isinstance(data, (list, tuple)) and isinstance(schema, StructType):
        col_names = [f.name for f in schema.fields]
        data = pd.DataFrame(list(data), columns=col_names)
    return _orig_create_df(self, data, schema=schema,
                           samplingRatio=samplingRatio, verifySchema=verifySchema)


SparkSession.createDataFrame = _arrow_create_df


@pytest.fixture(scope="session")
def spark():
    """Provides a local SparkSession for unit test execution.
    scope=session creates one session shared across all tests,
    avoiding the overhead of creating a new session per test.
    """
    session = (
        SparkSession.builder
        .master("local")
        .appName("fifa_unit_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
