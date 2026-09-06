"""Input adapters for source trips and the taxi-zone lookup."""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, functions as F

from batch.schemas import SOURCE_COLUMNS, SOURCE_CSV_SCHEMA, ZONE_LOOKUP_CSV_SCHEMA


def read_source_trips(
    spark: SparkSession, path: str, physical_format: str
) -> DataFrame:
    if physical_format == "csv":
        return (
            spark.read.option("header", True)
            .schema(SOURCE_CSV_SCHEMA)
            .csv(path)
        )
    if physical_format == "parquet":
        return _read_normalized_parquet_files(spark, path)
    raise ValueError(f"Unsupported source format: {physical_format}")


def _parquet_files(spark: SparkSession, path: str) -> list[str]:
    jvm = spark.sparkContext._jvm
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    root = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = root.getFileSystem(hadoop)
    iterator = filesystem.listFiles(root, True)
    files = []
    while iterator.hasNext():
        candidate = iterator.next().getPath().toString()
        if candidate.lower().endswith(".parquet"):
            files.append(candidate)
    if not files:
        raise FileNotFoundError(f"No Parquet files found under {path}")
    return sorted(files)


def _read_normalized_parquet_files(
    spark: SparkSession, path: str
) -> DataFrame:
    """Normalize TLC monthly schema drift before combining source files."""
    normalized_types = {
        "tpep_pickup_datetime": "timestamp",
        "tpep_dropoff_datetime": "timestamp",
        "PULocationID": "double",
        "DOLocationID": "double",
        "trip_distance": "double",
        "passenger_count": "double",
        "fare_amount": "double",
        "total_amount": "double",
    }
    frames = []
    for file_path in _parquet_files(spark, path):
        frame = spark.read.parquet(file_path)
        missing = sorted(set(SOURCE_COLUMNS) - set(frame.columns))
        if missing:
            raise ValueError(f"{file_path} is missing source columns: {missing}")
        frames.append(
            frame.select(
                *[
                    F.col(name).cast(normalized_types[name]).alias(name)
                    for name in SOURCE_COLUMNS
                ]
            )
        )
    return reduce(lambda left, right: left.unionByName(right), frames)


def read_zone_lookup(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(ZONE_LOOKUP_CSV_SCHEMA)
        .csv(path)
    )
