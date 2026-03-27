.PHONY: up down reset ingest clean dbt-run dbt-test dbt-docs

up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v
	docker compose up -d
	sleep 30

ingest:
	python pipelines/ingest_customer_addresses.py

clean:
	python cleaning/clean_tables.py

dbt-run:
	cd dbt && dbt run --full-refresh

dbt-test:
	cd dbt && dbt test

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve --port 8082

all: ingest clean dbt-run dbt-test
	@echo "✅ Full pipeline completed!"
