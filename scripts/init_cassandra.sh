#!/usr/bin/env bash
set -euo pipefail

HOST=cassandra
PORT=9042

echo "Waiting for Cassandra to accept connections on ${HOST}:${PORT}..."
while ! nc -z ${HOST} ${PORT}; do
  sleep 1
done

echo "Cassandra is up — running init CQL"
if command -v cqlsh >/dev/null 2>&1; then
  cqlsh ${HOST} ${PORT} -f /init_cassandra.cql
else
  # If cqlsh is not available in this container, attempt to run it via docker exec
  echo "cqlsh not found in this container. Please run: cqlsh ${HOST} ${PORT} -f /init_cassandra.cql"
fi

echo "Cassandra init complete"
#!/usr/bin/env bash
set -e

# Wait for cassandra to be ready
echo "Waiting for Cassandra to be ready..."
until cqlsh cassandra 9042 -e 'describe keyspaces' >/dev/null 2>&1; do
  sleep 2
done

# Apply init script
echo "Applying init_cassandra.cql"
cqlsh cassandra 9042 -f /init_cassandra.cql

echo "Cassandra initialized."
