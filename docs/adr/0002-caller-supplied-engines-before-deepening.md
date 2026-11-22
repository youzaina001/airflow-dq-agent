# Accept caller-supplied engines on the quality and audit path before the #52 deepening

LedgerLens (invoice-exception-workbench) pins this core and stamps its Reviewer scope by rebinding `make_engine` in three core modules at runtime. If a core change moves engine creation, that scope drops silently: row security hides every hosted staging row, and the completeness check returns `pass` with `0/0` (reproduced against core `5d602428` on 2026-11-22). #58 and #60 will move engine creation, so we add a small seam now, ahead of the #52 deepening order in #111. This adds a seam; it does not deepen anything, and it does not reopen ADR-0001.

The decision:

- `run_quality_suite` accepts a ready read engine and an audit sink. `record_quality_report` accepts the audit sink. `PostgresAuditSink` can be built on an engine. The caller owns engine lifetime.
- Only this path changes. Apply, target-set resolution, audit lookup, and admin setup keep their own engine creation until #58 and #60.
- Nothing supplied is silently ignored or replaced. A supplied audit sink is always written, whatever `TRACE_POSTGRES` says, and fails closed. The JSONL mirror keeps its current behavior. Supplying a read engine without an audit sink, or the reverse, is refused. Calls that supply neither behave exactly as before.

## Considered Options

- **Fold the seam into #58, #60, and #61 and keep the #111 order.** Rejected: every core release before #58 could break LedgerLens silently, and only a LedgerLens PostgreSQL test would notice.
- **Pass an engine factory** (DSN in, engine out). Rejected: the core keeps choosing DSNs, builds an engine per call, and still reads `AUDIT_DSN` from its own environment instead of the caller's settings.
- **A process-wide `set_engine_factory()` registration.** Rejected: it makes the monkeypatch official, stays global state invisible at the call site, and leaks between tests.
- **Accept engines at all ten engine-creation sites now.** Rejected: that is #58 and #60 done early under another name, and most of those sites would have only one adapter.
- **Let the supplied sink obey `TRACE_POSTGRES`, or let each missing piece fall back to its DSN default.** Rejected: both can silently swap a scoped write for an unscoped one or skip it entirely.

## Consequences

- The two adapters at this seam are the DSN default (CLI, DAGs, examples) and LedgerLens's Reviewer-scoped engines.
- LedgerLens bumps its pin, passes its scoped read and audit engines, and deletes `activate_connection_scope`. Its hosted `missing_amount.csv` test stays the end-to-end guard.
- #58 and #60 must build on this seam, not on DSNs, at these entry points.
- When #61 moves the audit write out of `run_quality_suite`, the audit sink parameter moves with it, and LedgerLens must still receive the report's `audit_event_id`.
