# airflow-dq-agent — local loop
# Default path never talks to an LLM and never mutates the warehouse.

PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
AIRFLOW_UID ?= $(shell id -u)
export LLM_MODE ?= stub
export APPLY_MODE ?= off
export AIRFLOW_UID
export WAREHOUSE_DSN ?= postgresql+psycopg://dq:dq@localhost:5433/warehouse
export TRACES_DIR ?= traces
OCR ?= ocr

.PHONY: help up down logs seed test eval demo lint fmt format typecheck check install ci catalog compose-smoke review-preview review review-branch compose-hitl

help:
	@echo "make install   - editable install with dev extras (no Airflow)"
	@echo "make up        - docker compose: Airflow 3.1 + warehouse Postgres"
	@echo "make down      - tear down compose (keep volumes)"
	@echo "make seed      - recreate synthetic warehouse + known defects"
	@echo "make test      - unit tests (no docker required)"
	@echo "make eval      - eval harness (stub + recorded traces, no live key)"
	@echo "make demo      - suite → stub propose → eval → print the story"
	@echo "make format    - auto-format and fix lint issues"
	@echo "make check     - lint, typecheck, and unit tests"
	@echo "make catalog   - run the bundled demo FastMCP catalog server"
	@echo "make compose-smoke - deterministic stub/shadow Compose DAG smoke test"
	@echo "make review-preview - list files OCR would review (no LLM call)"
	@echo "make review    - AI-review the workspace diff via OpenCodeReview"
	@echo "make review-branch - AI-review the current branch vs origin/master"
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
	$(PYTHON) -m airflow_dq_agent.demo.catalog

compose-smoke:
	bash scripts/compose-smoke.sh

compose-hitl:
	bash scripts/compose-hitl-reject-timeout.sh

lint:
	$(PYTHON) -m ruff check src tests dags examples
	$(PYTHON) -m ruff format --check src tests dags examples

fmt:
	$(PYTHON) -m ruff check --fix src tests dags examples
	$(PYTHON) -m ruff format src tests dags examples

format: fmt

typecheck:
	$(PYTHON) -m mypy src/airflow_dq_agent

check: lint typecheck test

ci: lint typecheck test eval

integration:
	$(PYTHON) -m pytest tests/integration -q --tb=short

# AI code review (OpenCodeReview). Setup and SDLC guidance: docs/ocr-code-review.md.
# These targets call the configured LLM; review-preview does not.
review-preview:
	$(OCR) review --preview

review:
	$(OCR) review

review-branch:
	$(OCR) review --from origin/master --to HEAD
