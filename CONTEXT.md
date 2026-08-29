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

**Apply Admission**:
The durable authorization to execute one evaluated remediation plan after its whole-plan human decision. It cannot authorize a different plan or report.
_Avoid_: Approval, permission

**Human Decision**:
An attributable, authorized response on one remediation plan. Approval and non-human timeout are distinct outcomes.
_Avoid_: Approval, button click
