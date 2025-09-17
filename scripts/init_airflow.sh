#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/init_airflow.sh [username] [password] [email]
# Defaults:
USERNAME=${1:-admin}
PASSWORD=${2:-admin}
EMAIL=${3:-admin@example.com}

echo "Initializing Airflow DB..."
docker compose run --rm airflow airflow db init

echo "Creating Airflow user: $USERNAME"
docker compose run --rm airflow airflow users create \
  --username "$USERNAME" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email "$EMAIL" \
  --password "$PASSWORD"

echo "Listing users:"
docker compose run --rm airflow airflow users list

echo "Done. You can now log in with $USERNAME / $PASSWORD"