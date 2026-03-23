# Compliance Rollout Follow-Up

Apply this follow-up on top of the current workspace state.

## Follow-Up Changes

1. `runbooks/sandbox-drill.md` is a sandbox-only drill.
   Remove any `## Audit Readiness` section from that file.
2. `customer-guides/release-faq.md` is a general FAQ.
   Remove any `## Audit Readiness` section from that file.
3. In `customer-guides/tenant-logging.md`, use `/var/log/tenant.log` as the
   production logging example path.
4. In `customer-guides/legacy-logging-guide.md`, use
   `legacy-compliance@company.example` as the escalation alias.
5. Update `reports/compliance-rollout-summary.md` so that it describes only the
   files changed in this follow-up round.

## Constraints

- Do not touch unrelated files.
- Keep the production rollout wording from the first pass.
- Preserve existing content outside the follow-up changes.
