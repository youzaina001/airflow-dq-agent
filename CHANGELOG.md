# Changelog

## 0.2.0 - 2026-09-06

### Added

- `register_contract`, `register_check`, and `load_registry` so a warehouse can
  declare table contracts and check specs without forking this repository.
- YAML loader validation errors name the file and the offending field.

### Changed

- The synthetic demo warehouse, fixtures, and seed live in
  `airflow_dq_agent.demo`. Importing the governance core no longer registers
  demo tables or checks.

## 0.1.0

Initial reference implementation.
