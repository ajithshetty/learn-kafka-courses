from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config.config import config_dict

spark: SparkSession = SparkSession.builder.appName(config_dict["spark"]).getOrCreate()

spark.conf.set("fs.s3a.endpoint", config_dict["s3"]["url"])
spark.conf.set("fs.s3a.access.key", config_dict["s3"]["access_key"])
spark.conf.set("fs.s3a.secret.key", config_dict["s3"]["secret_key"])
spark.conf.set("fs.s3a.path.style.access", "true")
spark.conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def topic_streams_writer(kafka_df):
    print("Writing starts", f"s3a://{config_dict['s3']['base_bucket']}landing")
    kafka_df \
        .select(col("key").cast("string"), col("value").cast("string")) \
        .writeStream \
        .format(config_dict["destination_format"]) \
        .trigger(processingTime="2 seconds") \
        .option("path", f"s3a://{config_dict['s3']['base_bucket']}landing") \
        .option("checkpointLocation", f"s3a://{config_dict['s3']['base_bucket']}checkpoint/")
    print("Writing completes", f"s3a://{config_dict['s3']['base_bucket']}landing")


def topic_streams_printer(kafka_df):
    print("Print starts", f"s3a://{config_dict['s3']['base_bucket']}landing/")
    kafka_df \
        .select(col("key").cast("string"), col("value").cast("string")) \
        .writeStream \
        .format("console") \
        .option("checkpointLocation", f"s3a://{config_dict['s3']['base_bucket']}checkpoint/")\
        .start()
    print("Print completes", f"s3a://{config_dict['s3']['base_bucket']}landing")


get_streams = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config_dict['kafka']["server"])
        .option("subscribe", config_dict['kafka']["topic"])
        .option("startingOffsets", config_dict.get("starting_offests", "earliest"))
        .load()
)

#topic_streams_printer(get_streams)

topic_streams_writer(get_streams)

spark.streams.awaitAnyTermination()
