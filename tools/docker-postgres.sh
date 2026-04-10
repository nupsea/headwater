#!/bin/bash
# Starts a local Postgres container for Headwater development
docker run -d \
  --name headwater-postgres \
  -e POSTGRES_USER=headwater \
  -e POSTGRES_PASSWORD=headwater \
  -e POSTGRES_DB=headwater_dev \
  -p 5434:5432 \
  postgres:16-alpine
echo "Postgres running at postgresql://headwater:headwater@localhost:5434/headwater_dev"
echo "Run: python tools/pg_ingest.py to load sample data"
