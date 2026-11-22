.PHONY: install generate run report test lint sql-check airflow all

BATCH ?= data/landing/batch_001

install:
	pip install -r requirements-dev.txt

generate:
	python -m scdw generate --out $(BATCH)

run:
	python -m scdw run $(BATCH)

report:
	python -m scdw report --out exports

all: generate run report

test:
	pytest --cov --cov-report=term-missing --cov-fail-under=85

lint:
	ruff check scdw dags tests

sql-check:
	python scripts/check_snowflake_sql.py

airflow:
	docker compose up --build
