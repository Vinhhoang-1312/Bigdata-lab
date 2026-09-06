from pyspark.sql import SparkSession

PATH = "s3a://urbanflow/test/spark-seaweedfs-roundtrip"
EXPECTED = [(1, "alpha"), (2, "beta"), (3, "gamma")]

spark = SparkSession.builder.appName("UrbanFlow S3A Smoke Test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

source = spark.createDataFrame(EXPECTED, ["id", "value"])
source.write.mode("overwrite").parquet(PATH)
actual = sorted((row.id, row.value) for row in spark.read.parquet(PATH).collect())
assert actual == EXPECTED, (actual, EXPECTED)

print("WRITE = PASS")
print("READ  = PASS")
print("DATA  = PASS")
print("PATH  =", PATH)
spark.stop()
