"""SparkSession factory."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from .paths import repo_root, use_local


def candidate_java_home(candidates: list[str | Path] | None = None) -> str | None:
    """Return the first candidate directory that contains bin/java(.exe)."""
    candidates = candidates or [
        Path(r"C:\Program Files\Android\Android Studio\jbr"),
        Path(r"C:\Program Files\Java\jdk-21"),
        Path(r"C:\Program Files\Java\jdk-17"),
        Path(r"C:\Program Files\Eclipse Adoptium\jdk-21"),
        Path(r"C:\Program Files\Eclipse Adoptium\jdk-17"),
    ]
    for item in candidates:
        home = Path(item)
        if (home / "bin" / "java.exe").exists() or (home / "bin" / "java").exists():
            return str(home)
    return None


def candidate_spark_home() -> str | None:
    exe = Path(sys.executable).resolve()
    candidates = [
        exe.parents[1] / "Lib" / "site-packages" / "pyspark" if len(exe.parents) > 1 else None,
        repo_root() / ".venv" / "Lib" / "site-packages" / "pyspark",
    ]
    for item in candidates:
        if item and (item / "jars").exists() and (item / "bin" / "spark-submit.cmd").exists():
            return str(item)
    return None


def configure_local_spark_environment() -> None:
    """Make PySpark robust on Windows laptops used for this assignment."""
    if not os.environ.get("SPARK_HOME"):
        spark_home = candidate_spark_home()
        if spark_home:
            os.environ["SPARK_HOME"] = spark_home
    java_home_env = os.environ.get("JAVA_HOME")
    java_home_valid = bool(java_home_env) and ((Path(java_home_env) / "bin" / "java.exe").exists() or (Path(java_home_env) / "bin" / "java").exists())
    if not java_home_valid:
        java_home = candidate_java_home()
        if java_home:
            os.environ["JAVA_HOME"] = java_home
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        java_bin = str(Path(java_home) / "bin")
        path_parts = os.environ.get("PATH", "").split(os.pathsep)
        if java_bin not in path_parts:
            os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")
    hadoop_home = repo_root() / ".hadoop"
    if not os.environ.get("HADOOP_HOME") and (hadoop_home / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["hadoop.home.dir"] = str(hadoop_home)
    hadoop_bin = str(hadoop_home / "bin")
    if (hadoop_home / "bin" / "hadoop.dll").exists() and hadoop_bin not in os.environ.get("PATH", "").split(os.pathsep):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    python_exe = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exe)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exe)


def get_spark(app_name: str = "BigEarthNetLab2"):
    configure_local_spark_environment()
    from pyspark.sql import SparkSession

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



