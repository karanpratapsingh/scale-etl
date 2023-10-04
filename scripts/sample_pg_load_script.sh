#!/bin/bash

# Run make postgres to start the local postgres container
SEGMENT_PATH=$1

PGHOST="localhost"
PGDATABASE="db"
PGUSER="user"
PGPASSWORD="pass"
export PGHOST PGDATABASE PGUSER PGPASSWORD

TABLE_NAME="products"

psql -c "CREATE TABLE IF NOT EXISTS $TABLE_NAME (id VARCHAR(255), name VARCHAR(255), price numeric, in_stock boolean);"
psql -c "\\COPY $TABLE_NAME FROM '$SEGMENT_PATH' WITH (FORMAT csv, HEADER true);"