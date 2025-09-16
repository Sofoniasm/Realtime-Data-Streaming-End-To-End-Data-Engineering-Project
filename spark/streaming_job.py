from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType


def main():
    spark = SparkSession.builder 
        .appName("kafka_to_cassandra") 
        .config("spark.cassandra.connection.host", "cassandra") 
        .getOrCreate()

    schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("username", StringType()),
        StructField("action", StringType()),
        StructField("value", DoubleType()),
        StructField("timestamp", LongType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .load()

    json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    query = json_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: batch_df.write.format("org.apache.spark.sql.cassandra").options(table="events", keyspace="events_keyspace").mode("append").save()) \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    main()
