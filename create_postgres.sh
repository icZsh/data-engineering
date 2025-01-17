#!/bin/bash

docker network create pg-network

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13

docker run -it \
	-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
	-e PGADMIN_DEFAULT_PASSWORD="root" \
	-p 8080:80 \
	--network=pg-network \
	--name pgadmin \
	dpage/pgadmin4

docker build -t taxi_ingest:v001 .

docker run -it \
  --network=pg-network \
  -v $(pwd):/app \
  taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pg-database \
  --db=ny_taxi \
  --table_name=yellow_taxi_data \
  --csv_name=/app/yellow_tripdata_2021-01.csv