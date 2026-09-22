# Governed Data Quality

This context defines how quality evidence may justify a proposed data remediation. It keeps deterministic checks, governed proposals, evaluation, and human approval distinct.

## Language

**Remediation Plan**:
A checked collection of remediation steps for one quality suite report, where every step is justified by one or more failed checks from that report.
_Avoid_: Fix list, action list

**Quality Evidence**:
A reference to a specific failed check in one quality suite report that justifies a remediation step.
_Avoid_: Citation, rationale

**Candidate Proposal**:
The typed agent output that requests remediations but has no authority until deterministic compilation produces a remediation plan.
_Avoid_: Remediation plan, approval

**Remediation Target Set**:
The exact contracted primary-key set selected for one remediation step. A human decision never authorizes rows outside that set.
_Avoid_: Current predicate, matching rows

**Check Policy**:
The declared quality rule that defines a check’s deterministic evidence, permitted remediation actions, and controlled execution values.
_Avoid_: Prompt rule, model preference

**Policy Snapshot**:
The exact contract, check policy, remediation rule, and renderer version that a remediation plan was compiled against.
_Avoid_: Latest configuration, current rule

**Audit Lineage**:
The immutable association from a quality suite report through its remediation plan, evaluation, human decision, and apply result.
_Avoid_: Log, history

**Quarantine**:
A copy of the authorized Remediation Target Set into a holding table. It is not a repair of source rows.
_Avoid_: Fix, repair, source update

**Shadow Review**:
An evaluated, sample-free Remediation Plan review recorded in Audit Lineage without a Human Decision or Apply Admission.
_Avoid_: Approval, dry run, shadow skip

**Apply Admission**:
The durable authorization to execute one evaluated Remediation Plan against its Remediation Target Set once, after its whole-plan Human Decision. Retrying a committed Apply Admission returns the original committed result from Audit Lineage with the same identity and counts; this recovery is read-only, even after expiry. It cannot authorize a second mutation or a different plan or report. An unconsumed expired Apply Admission is refused.
_Avoid_: Approval, permission

**Human Decision**:
An attributable, authorized response on one evaluated remediation plan, bound to that plan's identity and Audit Lineage event. Approval and non-human timeout are distinct outcomes.
_Avoid_: Approval, button click

**Decision Recording**:
The one validated path that records a Human Decision: validate, fingerprint, persist Audit Lineage, then attach the audit event id. A fully hand-forged Human Decision that already matches Audit Lineage would still pass Apply Admission.
_Avoid_: Unvalidated persist, hand-stitched audit id

**Decision Fingerprint**:
The canonical digest of a Human Decision payload covering decision_id, outcome, actor, note, and decided_at.
_Avoid_: Review fingerprint, audit event id
