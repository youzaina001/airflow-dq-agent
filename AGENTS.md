# Airflow DQ Agent

## Navigation

- Start with `CONTEXT.md` for domain terminology and `docs/` (`docs/adr/`, `docs/ocr-code-review.md`, `docs/release-evidence-slice1.md`) for architectural decisions and evidence notes.
- Python sources live across `src/`, `tests/`, `dags/`, `examples/`, and `scripts/`, while `evals/` holds YAML cases and JSON fixtures (also checked by Ruff).

## Checks and hooks

- Run `make check` (`lint`, `typecheck`, `tests/unit`) or `make ci` (also `tests/evals`) via `Makefile`.
- Install git hooks locally with `make hooks` (enforces single-line commit messages and blocks AI attribution trailers via `.githooks/commit-msg`).
- `pyproject.toml` selects `"RUF"` (which includes `RUF100` unused-noqa checking). `tests/unit/test_traces.py` retains `# noqa: F401` intentionally because `append_human_decision` is checked as an import seam and triggers `F401` if removed.
