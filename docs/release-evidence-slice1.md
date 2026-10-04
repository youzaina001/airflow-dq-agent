Slice-1 release evidence checklist

Candidate commit
- 438320daa0c9ebf821d53d16cad456dd1c802a4e
- This is the commit that adds scripts/check_branch_coverage.py and the branch-coverage job in .github/workflows/ci.yml.

Commands
- cd /home/youzaina001/agy-projects/projects/airflow-dq-agent-slice1
- PYTHONPATH=src LLM_MODE=stub APPLY_MODE=off /home/youzaina001/agy-projects/projects/airflow-dq-agent/.venv/bin/python -m pytest tests/unit tests/evals -q --tb=short --cov=airflow_dq_agent --cov-branch --cov-report=json:coverage.json --cov-report=
- PYTHONPATH=src /home/youzaina001/agy-projects/projects/airflow-dq-agent/.venv/bin/python scripts/check_branch_coverage.py coverage.json

Results
- pytest tests/unit tests/evals exited 0. LLM_MODE=stub and APPLY_MODE=off. No live LLM key was used.
- The checker exited 0.
- covered_branches/num_branches 451/580
- ratio 0.77758621
- 451/580 is at least 75%. The metric is sum(covered_branches) / sum(num_branches) from the coverage JSON with branch collection on. It is not coverage.py percent_covered.
- The JSON report was coverage.json. CI uploads that file from the branch-coverage job as the coverage-json artifact.

Coverage scope
- Every Python file under src/airflow_dq_agent/planning/
- Every Python file under src/airflow_dq_agent/apply/
- Every Python file under src/airflow_dq_agent/contracts/
- Every Python file under src/airflow_dq_agent/quality/ except src/airflow_dq_agent/quality/pandera_schemas.py
- Every Python file under src/airflow_dq_agent/traces/
- Every Python file under src/airflow_dq_agent/evals/
- Plus src/airflow_dq_agent/action_definitions.py, src/airflow_dq_agent/check_policy.py, src/airflow_dq_agent/hitl.py, src/airflow_dq_agent/airflow_hitl.py, and src/airflow_dq_agent/agent/sanitize.py
- New Python files under those directories are included. The only exclusion is src/airflow_dq_agent/quality/pandera_schemas.py.

Workflow
- https://github.com/youzaina001/airflow-dq-agent/actions/workflows/ci.yml
- Job name: branch-coverage
- Workflow file: .github/workflows/ci.yml
- Required jobs that stay in that workflow: lint-and-typecheck, unit, eval, integration, compose-smoke, compose-shadow, compose-hitl, and branch-coverage.
- A skipped or failed required CI job is incomplete acceptance.

Release claims
- This release does not claim null-fill or source repair. Issue #10 is not in commit 438320daa0c9ebf821d53d16cad456dd1c802a4e.
- This checklist contains no database password and no API key.
