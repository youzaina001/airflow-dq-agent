# Finish governed adoption before remaining deepening

Milestone 5 still required configured shadow (#68) to wait on fingerprint and audit-facade deepening (#55–#61) because the old issue graph said so, not because a shadow Remediation Plan review needs those modules. Adoption is the next stretch: committed-result recovery (#29) then Airflow crash/retry (#42), then a thin sequencing wrapper over existing `compile_remediation_plan`, `evaluate_plan`, `build_approval_review`, and `append_event` (#68) called by CLI and Airflow, then configured shadow (#69). Remaining #52 deepenings and supporting tickets (#4, #5, #7, #31, #32, #76) stay post-adoption.

## Considered Options

- **Deepen first** — land #55–#61 (and then #57/#58) so CLI and Airflow share one persistence/fingerprint seam. Rejected: it delays the adopter-visible proof behind a five-ticket refactor.
- **Stop after crash/retry** — park #65/#68/#69. Rejected: configured shadow is still the other half of the adoption promise; it does not require the deepening chain.
- **Deep preparation module now** — hide compile/eval/review/lineage behind a new persistence owner. Rejected: that recreates #60/#61 under another name.
- **Treat full #65 as the adoption gate** — keep one report root and atomic report/check recording on the critical path. Rejected after review: those guarantees stay with #59–#61; this stretch is the narrow completeness/quarantine journey.

## Consequences

- #68 is not blocked on #61. It must not add a fingerprint mixin, durable-projection module, or Audit Trail facade.
- #52 remains the architecture reference; it is not the critical path for this stretch.
- PR #77 (#29) stays open until an explicit merge decision.
- Until this adoption stretch is proven, apply is quarantine-only: a Human Decision authorizes a copy of the Remediation Target Set, not source repair. `null_fill` (#10) stays parked.
- The product has two callers: CLI for Shadow Review, Airflow for Human Decision and apply. #68 exists so those callers do not drift.
- A live model is an optional Candidate Proposal source behind the same seam. Stub is the default and the adoption path. #6 is not this stretch.
- Adoption proven for this stretch is a **narrow completeness/quarantine journey**: one table, one primary key, `quarantine_nulls`. The gate is #29, #42, and #69 green. That is not full #65 acceptance. One report root, atomic report/check recording, and event replay stay with #59–#61 after this stretch. Predicate parity (#4) and referenced-contract Policy Snapshot (#32) are outside this claim.
- #69 Shadow Review must persist PostgreSQL Audit Lineage through the restricted audit login. JSONL-only (`TRACE_POSTGRES=false`) is a local demonstration, not adoption success.
- Entry points load one selected registry per process. Accumulating demo plus adopter catalogs is not a passing #69.
- #68 reuses the existing report root and candidate predecessor. It must not record a second root via `trace_agent_run()`.
- PostgreSQL is the product warehouse. Target resolution has an internal compiler seam; PostgreSQL is the production adapter and tests supply fakes. The exported `TargetSetResolver` interface does not promise another warehouse.
- Distinct read, audit, and apply PostgreSQL logins are the adopter contract, not a demo. Shadow Review uses read and audit; apply stays off. Pointing all three at a superuser is not success.
- Check Policy is the reviewed law for what may be compiled. Proposal/plan evaluation is an extra refusal layer, not a second source of SQL. Do not collapse them in this stretch. Human Decision remains whole-plan.
