from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType

# We'll use the Python cassandra-driver in foreachBatch to write rows to Cassandra.
# Add robust logging and error handling so we can see what's happening in the container logs.
from cassandra.cluster import Cluster


def main():
    spark = (SparkSession.builder
             .appName("kafka_to_cassandra")
             .config("spark.cassandra.connection.host", "cassandra")
             # we rely on the Python cassandra-driver (installed in the image) and
             # the Kafka connector provided via spark-submit --packages
             .getOrCreate())

    # reduce Spark noise
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("user_id", IntegerType()),
        StructField("username", StringType()),
        StructField("action", StringType()),
        StructField("value", DoubleType()),
        StructField("timestamp", LongType())
    ])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .load()

    json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    def write_to_cassandra(batch_df, batch_id):
        try:
            # log batch start and size
            count = batch_df.count()
            print(f"[write_to_cassandra] batch_id={batch_id} row_count={count}", flush=True)
            if count == 0:
                return

            # collect rows (acceptable for demo volumes). For production, use a distributed writer.
            rows = batch_df.select("user_id", "timestamp", "action", "username", "value").collect()

            cluster = Cluster(['cassandra'])
            session = cluster.connect('events_keyspace')
            insert_cql = "INSERT INTO events (user_id, timestamp, action, username, value) VALUES (?, ?, ?, ?, ?)"
            prepared = session.prepare(insert_cql)

            inserted = 0
            for r in rows:
                try:
                    session.execute(prepared, (int(r['user_id']), int(r['timestamp']), r['action'], r['username'], float(r['value'])))
                    inserted += 1
                except Exception as e:
                    # log row-level failures but continue
                    print(f"[write_to_cassandra] row insert failed: {e}; row={r}", flush=True)

            session.shutdown()
            cluster.shutdown()
            print(f"[write_to_cassandra] batch_id={batch_id} inserted={inserted}", flush=True)
        except Exception as e:
            # log any batch-level error to container logs
            print(f"[write_to_cassandra] Exception while writing batch {batch_id}: {e}", flush=True)

    query = json_df.writeStream \
        .foreachBatch(write_to_cassandra) \
        .start()

    print("Streaming query started; awaiting termination", flush=True)
    query.awaitTermination()


if __name__ == '__main__':
    main()
