PY ?= python
AIRFLOW_VERSION ?= 3.2.2
PYTHON_VERSION := $(shell $(PY) -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
CONSTRAINTS := https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt

.PHONY: install data test e2e lint fmt up down clean

install:  ## Airflow (pinned with its official constraints) + this package
	$(PY) -m pip install "apache-airflow==$(AIRFLOW_VERSION)" -e ".[dev]" --constraint "$(CONSTRAINTS)"

data:     ## 3 days of synthetic AMI data, with one region's file truncated on day 2
	$(PY) data_generator/generate_ami_data.py --root data --start-date 2026-11-02 --days 3 \
		--truncate NORTH_COAST:2026-11-03

test:     ## Unit tests + DAG integrity tests
	$(PY) -m pytest -q

e2e:      ## Run the real DAGs end to end with dag.test(): failure, re-send, recovery, report
	$(PY) scripts/run_e2e.py --root /tmp/ami-e2e

lint:
	ruff check . && ruff format --check .

fmt:
	ruff check --fix . && ruff format .

up:       ## Airflow UI at http://localhost:8080
	docker compose up --build

down:
	docker compose down

clean:
	rm -rf data airflow-home .pytest_cache
