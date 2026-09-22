from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def foundation():
    return Path(__file__).parents[2] / "urbanflow-foundation"


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    session = (pyspark.sql.SparkSession.builder.master("local[2]")
               .appName("urbanflow-tests").config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    session.conf.set("spark.sql.session.timeZone", "America/New_York")
    yield session
    session.stop()
