# airflow-dq-agent — local loop
# Default path never talks to an LLM and never mutates the warehouse.

PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
AIRFLOW_UID ?= $(shell id -u)
export LLM_MODE ?= stub
export APPLY_MODE ?= off
export AIRFLOW_UID
export WAREHOUSE_DSN ?= postgresql+psycopg://dq:dq@localhost:5433/warehouse
export TRACES_DIR ?= traces

.PHONY: help up down logs seed test eval demo lint fmt typecheck install ci catalog compose-smoke

help:
	@echo "make install   - editable install with dev extras (no Airflow)"
	@echo "make up        - docker compose: Airflow 3.1 + warehouse Postgres"
	@echo "make down      - tear down compose (keep volumes)"
	@echo "make seed      - recreate synthetic warehouse + known defects"
	@echo "make test      - unit tests (no docker required)"
	@echo "make eval      - eval harness (stub + recorded traces, no live key)"
	@echo "make demo      - suite → stub propose → eval → print the story"
	@echo "make catalog   - run the FastMCP catalog server"
	@echo "make compose-smoke - deterministic stub/shadow Compose DAG smoke test"
	@echo "make lint fmt typecheck ci"

install:
	$(PYTHON) -m pip install -e ".[dev]"

up:
	mkdir -p logs plugins config dags traces
	test -f .env || cp .env.example .env
	docker compose up -d --build
	@echo "Airflow UI: http://localhost:8080  (airflow / airflow)"
	@echo "Warehouse : localhost:5433  (dq / dq / warehouse)"
	@echo "Then: make seed && open the dq_daily DAG (paused by default)."

down:
	docker compose down

logs:
	docker compose logs -f airflow-apiserver airflow-scheduler airflow-dag-processor

seed:
	$(PYTHON) -m airflow_dq_agent.cli seed

test:
	LLM_MODE=stub $(PYTHON) -m pytest tests/unit -q

eval:
	LLM_MODE=stub $(PYTHON) -m pytest tests/evals -q --tb=short

demo:
	LLM_MODE=stub APPLY_MODE=off $(PYTHON) -m airflow_dq_agent.cli demo --no-db

catalog:
	$(PYTHON) -m airflow_dq_agent.catalog.mcp_server

compose-smoke:
	bash scripts/compose-smoke.sh

lint:
	$(PYTHON) -m ruff check src tests dags
	$(PYTHON) -m ruff format --check src tests dags

fmt:
	$(PYTHON) -m ruff check --fix src tests dags
	$(PYTHON) -m ruff format src tests dags

typecheck:
	$(PYTHON) -m mypy src/airflow_dq_agent

ci: lint typecheck test eval

integration:
	$(PYTHON) -m pytest tests/integration -q --tb=short
