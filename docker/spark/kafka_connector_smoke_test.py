from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder.appName("UrbanFlow Kafka Connector Smoke Test")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    try:
        frame = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "urbanflow.trip-events")
            .load()
        )
        fields = set(frame.schema.fieldNames())
        required = {"key", "value", "topic", "partition", "offset", "timestamp"}
        missing = sorted(required - fields)
        if missing:
            raise AssertionError(f"Kafka source schema missing fields: {missing}")
        print("KAFKA CONNECTOR = PASS")
        print("FIELDS = " + ",".join(frame.schema.fieldNames()))
        print("TIMEZONE = " + spark.conf.get("spark.sql.session.timeZone"))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
