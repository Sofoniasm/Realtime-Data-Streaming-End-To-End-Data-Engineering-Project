# Real-time Kafka -> Spark -> Cassandra pipeline (Docker)

This project demonstrates a local end-to-end streaming pipeline using Docker Compose.

![architecture](assets/architecture.svg)

Services:
- Zookeeper
- Kafka
- Schema Registry
- Cassandra
- Spark (master + worker)
- Airflow
- Producer (Python)

How to run (Linux / WSL / Docker Desktop):

1. Start the stack:

```bash
docker compose up -d --build
```

2. Initialize Cassandra (once):

```bash
docker exec -i $(docker ps -qf "name=cassandra") cqlsh -f /init_cassandra.cql
```

3. Start the producer (sends events to topic `events`):

```bash
docker compose run --rm producer
```

4. Submit Spark job (example):

```bash
./scripts/spark_submit.sh
```

Airflow DAG: `airflow/dags/kafka_to_cassandra_dag.py` will run the producer and then call the spark submit helper.

Verify: connect to Cassandra and query `SELECT * FROM events_keyspace.events LIMIT 10;`.

Notes:
- This is a minimal demo for local development. Production deployments need more secure configs, proper replication, and resource tuning.

Troubleshooting:
- If Kafka clients cannot connect, ensure the broker advertised listeners are reachable from your host. Try using `localhost:29092` from host tools.
- If Spark cannot find the Cassandra connector, you may need to add the connector jar to the Spark image or use the Datastax Spark Cassandra connector package with spark-submit `--packages`.
- Airflow may require additional configuration (init DB, users). For quick tests, run the `airflow` container and open `http://localhost:8082`.

Verification steps:
1. Start the stack: `docker compose up -d --build`
2. Initialize Cassandra: `docker exec -i $(docker ps -qf "name=cassandra") cqlsh -f /init_cassandra.cql`
3. Run the producer for a short time: `docker compose run --rm producer` (Ctrl+C to stop)
4. Submit Spark job: `./scripts/spark_submit.sh`
5. Query Cassandra: `docker exec -it $(docker ps -qf "name=cassandra") cqlsh -e "SELECT * FROM events_keyspace.events LIMIT 10;"`

Next steps / improvements:
- Add schema-registry Avro serialization and use Confluent serializers.
- Harden Docker images and add resource limits for Spark and Cassandra.
- Create automated integration tests that run the full stack and assert rows in Cassandra.
