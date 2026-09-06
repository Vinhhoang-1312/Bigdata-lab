from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("UrbanFlow Spark Smoke Test")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

assert spark.conf.get("spark.sql.session.timeZone") == "America/New_York"
rows = spark.createDataFrame(
    [(1, "ok"), (2, "ready")], ["id", "status"]
).collect()
assert [(row.id, row.status) for row in rows] == [(1, "ok"), (2, "ready")]

print("SPARK = PASS")
print("MASTER =", spark.sparkContext.master)
print("TIMEZONE =", spark.conf.get("spark.sql.session.timeZone"))
spark.stop()
