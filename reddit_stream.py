from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
)

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Reddit demo")
        .master("local[3]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("submission_id", StringType()),
            StructField("subreddit", StringType()),
            StructField("creation_time", StringType()),
            StructField("title", StringType()),
            StructField("author", StringType()),
            StructField("is_NSFW", BooleanType()),
            StructField("upvotes", IntegerType()),
            StructField("url", StringType()),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "redditpost")
        .option("startingOffsets", "earliest")
        .load()
    )

    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))

    explode_df = value_df.selectExpr(
        "value.submission_id",
        "value.subreddit",
        "value.creation_time",
        "value.title",
        "value.author",
        "value.is_NSFW",
        "value.upvotes",
        "value.url",
    ).withColumn("creation_time", to_timestamp("creation_time"))

    deduped_df = explode_df.withWatermark("creation_time", "1 minute").dropDuplicates(
        ["submission_id", "creation_time"]
    )

    kafka_df_writer_query = (
        deduped_df.writeStream.format("console")
        .queryName("reddit stream generator")
        .outputMode("append")
        .option("checkpointLocation", "chk-point-dir")
        .trigger(processingTime="1 minute")
        .start()
    )

    kafka_df_writer_query.awaitTermination()
