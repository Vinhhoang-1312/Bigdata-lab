"""SparkSession factory."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from .paths import repo_root, use_local


def get_spark(app_name: str = "BigEarthNetLab2") -> SparkSession:
    # Ensure `import scripts.lib...` works on driver + local workers
    root = str(repo_root())
    existing = os.environ.get("PYTHONPATH", "")
    if root not in existing.split(os.pathsep):
        os.environ["PYTHONPATH"] = root + (os.pathsep + existing if existing else "")
    if root not in sys.path:
        sys.path.insert(0, root)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", os.environ.get("BEN_SHUFFLE_PARTITIONS", "32"))
        .config("spark.executorEnv.PYTHONPATH", os.environ["PYTHONPATH"])
        .config("spark.driver.extraPythonPath", root)
        .config("spark.executor.extraPythonPath", root)
    )

    master = os.environ.get("SPARK_MASTER")
    if master:
        builder = builder.master(master)
    elif use_local():
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("BEN_LOG_LEVEL", "WARN"))
    return spark
