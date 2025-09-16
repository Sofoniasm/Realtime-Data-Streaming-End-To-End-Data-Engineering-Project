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
