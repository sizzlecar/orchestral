# Compliance Rollout Batch Update

## Purpose

This scenario is designed for product comparison videos where the goal is to show runtime advantages without relying on binary artifact specialists such as spreadsheet-specific tooling.

The first runnable repository version should be document-heavy. A later extension can add structured configs once the mixed-family path is ready for larger fan-out.

The scenario is intentionally grounded in a realistic enterprise workflow:

- one change request
- many markdown files
- many YAML/JSON configuration files
- one generated summary report
- a second pass with narrower follow-up feedback

The core comparison target is not "who can write text", but:

- who can scan many files correctly
- who can update only the affected files
- who can verify the result
- who can rerun with minimal rework

## Industry Context

Target context: enterprise SaaS / fintech / healthcare platform compliance rollout.

A security or compliance team sends a change request before a release window. The engineering team must:

- unify production logging policy
- enable audit-related defaults
- update operational runbooks
- update customer-facing deployment guides
- generate a rollout summary for audit review

This is a common real-world batch update pattern. It naturally spans dozens of files and often comes in two passes:

1. initial rollout
2. compliance/legal follow-up corrections

## Artifact Families

Target shape:

- `document`
  - markdown runbooks
  - markdown customer deployment guides
  - generated markdown summary report

Optional later extension:

- `structured`
  - YAML service configs
  - JSON tenant defaults

No Excel, PDF, or binary office documents should be used in the primary comparison version.

## Suggested Workspace Layout

```text
fixtures/scenarios/compliance_rollout_batch_update/
├── instructions/
│   ├── compliance-change-request.md
│   └── compliance-followup.md
├── runbooks/
│   ├── oncall-escalation.md
│   ├── release-rollout.md
│   ├── incident-triage.md
│   └── ...
├── customer-guides/
│   ├── production-deployment.md
│   ├── audit-mode.md
│   ├── observability.md
│   └── ...
└── reports/
    └── compliance-rollout-summary.md
```

Recommended scale for the first runnable video version:

- `12-18` runbook markdown files
- `8-12` customer guide markdown files
- `1` summary report

This is large enough to create meaningful scanning and rerun pressure, but not so large that the scenario becomes slow for the wrong reasons.

## Round 1 Business Request

User request:

```text
读取 instructions/compliance-change-request.md，
按其中要求批量更新当前工作区的配置文件和文档，
最后把本次真实发生的改动写入 reports/compliance-rollout-summary.md。
```

The request file should describe realistic rollout requirements such as:

1. Runbooks:
   - replace old `staging rollout` wording with `production rollout`
   - add one audit note section if missing
2. Customer guides:
   - replace old debug/logging wording with production logging wording
3. Summary report:
   - list only files that actually changed
   - group them by `runbook`, `customer-guide`

## Why This Is Realistic

This models a genuine pre-release compliance sweep:

- security policy update
- production baseline unification
- audit readiness
- documentation drift repair

It does not depend on hidden fixture trivia. The system must inspect the repository and determine:

- which files are relevant
- which files actually need changes
- which files must remain untouched

## Round 2 Follow-Up

Round 2 should represent a normal compliance/legal follow-up, not a test trick.

User request:

```text
读取 instructions/compliance-followup.md，
在上一轮结果基础上继续收敛整改，
只修改本轮新增要求涉及的文件，并更新 reports/compliance-rollout-summary.md。
```

Follow-up file examples:

- `sandbox-*` tenants must keep `audit_mode: false`
- customer guides for legacy customers must keep the old field name `log_path`
- only production-facing runbooks need the new audit note
- summary should only describe this round's incremental changes

This second pass is where runtime differences become much more visible.

## Expected Comparison Pressure

This scenario naturally pressures five areas:

1. file discovery at scale
2. large-batch document consistency
3. change targeting instead of blanket rewrites
4. summary accuracy versus real artifact changes
5. minimal rework on the second pass

## Verification Design

Verification should be layered and machine-checkable.

### Round 1

- document verifier:
  - required sections/text exist
  - no forbidden legacy wording remains in targeted files
- summary verifier:
  - summary mentions only changed files
  - categories match actual file classes

### Round 2

- all round-1 unaffected files must remain byte-identical
- only follow-up-affected files may differ
- summary must describe only round-2 incremental changes

## What Makes This Fair For Comparison

This scenario avoids the main fairness problem of the spreadsheet benchmark:

- no binary artifact specialization
- no office-specific built-in adapter advantage
- both sides operate on markdown text files

The gap, if any, is therefore much more likely to come from runtime behavior:

- staged execution
- bounded derivation
- verification discipline
- rerun minimality

## What The Video Should Show

The comparison should not focus on chat transcript quality.

It should show:

- wall-clock time
- number of files changed
- whether structured verifiers passed
- whether document verifiers passed
- whether summary matched real changes
- whether round-2 rework stayed minimal

## Recommended Success Narrative

If Orchestral wins this scenario, the message should be:

"It is not winning because it has a special file-format tool. It is winning because the runtime is better at large-scale, cross-family, verify-gated repo updates."
