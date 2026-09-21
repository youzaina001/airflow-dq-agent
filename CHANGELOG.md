# Changelog

## Unreleased

### Changed

- OpenCodeReview CI disables background self-updates so the pinned CLI
  stays at 1.12.7, and reports installation or execution errors as failed
  checks instead of masking them as successful reviews.
- OpenCodeReview job timeout is 60 minutes and the per-group review
  deadline is 30 minutes, so grouped OpenRouter reviews are less likely
  to abort as `timeout`. The dispatch dropdown is `z-ai/glm-5.3-flash`,
  `z-ai/glm-5.3-flashx`, and `deepseek/deepseek-v4.1-flash`, with a
  `reasoning_effort` input (default `low`).

### Added

- OpenCodeReview (`ocr`) integration: project review rules in
  `.opencodereview/rule.json`, `make review*` targets, and an on-demand
  OpenRouter-backed PR workflow (manual `workflow_dispatch` with a model
  dropdown, or `/ocreview` on a pull request). See `docs/ocr-code-review.md`.
- Packaged external-invoice adopter example: `register_external_invoice`,
  restricted read/audit/apply login provisioning, and
  `examples/dq_external_invoice.py` for Airflow quarantine copies.

## 0.2.0 - 2026-09-06

### Added

- `register_contract`, `register_check`, and `load_registry` so a warehouse can
  declare table contracts and check specs without forking this repository.
- YAML loader validation errors name the file and the offending field.

### Changed

- The synthetic demo warehouse, fixtures, and seed live in
  `airflow_dq_agent.demo`. Importing the governance core no longer registers
  demo tables or checks.
- Compose and `make catalog` run `python -m airflow_dq_agent.demo.catalog`,
  which registers the synthetic warehouse before starting the generic MCP
  server. `airflow_dq_agent.catalog.mcp_server` still starts with empty
  catalogs.

## 0.1.0

Initial reference implementation.
