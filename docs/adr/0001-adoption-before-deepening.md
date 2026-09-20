# Finish governed adoption before remaining deepening

Milestone 5 still required configured shadow (#68) to wait on fingerprint and audit-facade deepening (#55–#61) because the old issue graph said so, not because a shadow Remediation Plan review needs those modules. Adoption is the next stretch: committed-result recovery (#29) then Airflow crash/retry (#42), then a thin sequencing wrapper over existing `compile_remediation_plan`, `evaluate_plan`, `build_approval_review`, and `append_event` (#68) called by CLI and Airflow, then configured shadow (#69). Remaining #52 deepenings and supporting tickets (#4, #5, #7, #31, #32, #76) stay post-adoption.

## Considered Options

- **Deepen first** — land #55–#61 (and then #57/#58) so CLI and Airflow share one persistence/fingerprint seam. Rejected: it delays the adopter-visible proof behind a five-ticket refactor.
- **Stop after crash/retry** — park #65/#68/#69. Rejected: configured shadow is still the other half of the adoption promise; it does not require the deepening chain.
- **Deep preparation module now** — hide compile/eval/review/lineage behind a new persistence owner. Rejected: that recreates #60/#61 under another name.

## Consequences

- #68 is not blocked on #61. It must not add a fingerprint mixin, durable-projection module, or Audit Trail facade.
- #52 remains the architecture reference; it is not the critical path for this stretch.
- PR #77 (#29) stays open until an explicit merge decision.
- Until this adoption stretch is proven, apply is quarantine-only: a Human Decision authorizes a copy of the Remediation Target Set, not source repair. `null_fill` (#10) stays parked.
- The product has two callers: CLI for Shadow Review, Airflow for Human Decision and apply. #68 exists so those callers do not drift.
- A live model is an optional Candidate Proposal source behind the same seam. Stub is the default and the adoption path. #6 is not this stretch.
- Adoption is proven only when #29, #42, and #69 are green: committed retry, Airflow crash/retry with a single copy, and a CLI Shadow Review of an adopter registry without DAG source edits. Until then, do not start #10 or remaining #52 deepening as a new bet.
- PostgreSQL is the product warehouse. `TargetSetResolver` is an internal test fake, not a public port. Do not start a second warehouse adapter.
- Distinct read, audit, and apply PostgreSQL logins are the adopter contract, not a demo. Shadow Review uses read and audit; apply stays off. Pointing all three at a superuser is not success.
- Check Policy is the reviewed law for what may be compiled. Proposal/plan evaluation is an extra refusal layer, not a second source of SQL. Do not collapse them in this stretch. Human Decision remains whole-plan.
