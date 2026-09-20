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
