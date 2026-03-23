# Compliance Rollout Change Request

Apply this compliance rollout update to the current workspace.

## Scope

- Update markdown files under `runbooks/`.
- Update markdown files under `customer-guides/`.
- Write the final changed-file summary to `reports/compliance-rollout-summary.md`.

## Required Changes

1. Add a top-level H1 title to any markdown file that does not already have one.
   Deriving the title from the filename is acceptable.
2. Add an `## Audit Readiness` section to every production-facing runbook and
   customer guide that does not already have one.
   Keep the section short and practical.
3. In customer-facing guides, standardize the escalation alias to
   `audit-review@company.example`.
4. Update `reports/compliance-rollout-summary.md` so that it lists only files
   that actually changed in this round.
   Use relative paths and group them under `## Runbooks` and
   `## Customer Guides`.

## Constraints

- Do not rewrite already compliant files.
- Preserve unrelated content and structure.
- Do not modify files outside `runbooks/`, `customer-guides/`, and
  `reports/compliance-rollout-summary.md`.
