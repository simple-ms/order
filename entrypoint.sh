#!/bin/bash
set -e

echo "Waiting for database..."
sleep 5

echo "Running Alembic migrations..."
alembic upgrade head

echo "Starting application..."
exec "$@"
