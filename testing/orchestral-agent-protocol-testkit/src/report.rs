use std::fmt;

/// Stable identifier for a conformance assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CaseId(&'static str);

impl CaseId {
    pub const fn new(value: &'static str) -> Self {
        Self(value)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for CaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// IDs are protocol-test API. Existing values must never be renamed or reused.
pub mod case_ids {
    use super::CaseId;

    pub const DESCRIPTOR_START_BINDING: CaseId =
        CaseId::new("m0.provider.descriptor_start_binding.v1");
    pub const SAME_RUN_START_IDEMPOTENT: CaseId =
        CaseId::new("m0.provider.same_run_start_idempotent.v1");
    pub const RUN_ID_DIGEST_CONFLICT: CaseId = CaseId::new("m0.provider.run_id_digest_conflict.v1");
    pub const ATOMIC_IMMEDIATE_COMPLETION: CaseId =
        CaseId::new("m0.provider.atomic_immediate_completion.v1");
    pub const RECOVER_UNSUPPORTED_WHEN_UNDECLARED: CaseId =
        CaseId::new("m0.provider.recover_unsupported_when_undeclared.v1");
    pub const RECOVER_STABLE_WHEN_DECLARED: CaseId =
        CaseId::new("m0.provider.recover_stable_when_declared.v1");
    pub const SESSION_CONFLICT_WHEN_REUSE_UNDECLARED: CaseId =
        CaseId::new("m0.provider.session_conflict_when_reuse_undeclared.v1");
    pub const SESSION_ISOLATION_WHEN_REUSE_DECLARED: CaseId =
        CaseId::new("m0.provider.session_isolation_when_reuse_declared.v1");
    pub const OUTCOME_UNKNOWN_IDENTITY_RETRY_1000: CaseId =
        CaseId::new("m0.provider.outcome_unknown_identity_retry_1000.v1");

    /// The original first slice remains stable for downstream users that pin
    /// its IDs. The complete base suite uses a capability-dependent case set.
    pub const FIRST_SLICE: [CaseId; 5] = [
        DESCRIPTOR_START_BINDING,
        SAME_RUN_START_IDEMPOTENT,
        RUN_ID_DIGEST_CONFLICT,
        ATOMIC_IMMEDIATE_COMPLETION,
        RECOVER_UNSUPPORTED_WHEN_UNDECLARED,
    ];
}

/// A case is successful only when the invariant was positively observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaseVerdict {
    Passed,
    Failed { reason: String },
    NotProven { reason: String },
}

impl CaseVerdict {
    pub const fn is_passed(&self) -> bool {
        matches!(self, Self::Passed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseResult {
    pub case_id: CaseId,
    pub verdict: CaseVerdict,
}

/// Deterministically ordered result of one suite run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConformanceReport {
    expected_cases: Vec<CaseId>,
    cases: Vec<CaseResult>,
}

impl ConformanceReport {
    /// Constructs a report for the original fixed first-slice case set.
    pub fn new(cases: Vec<CaseResult>) -> Self {
        Self::for_cases(case_ids::FIRST_SLICE, cases)
    }

    /// Constructs a report whose complete case set is explicit. This is used
    /// when descriptor capabilities select mutually-exclusive positive and
    /// negative contract cases; neither branch is skipped.
    pub fn for_cases(
        expected_cases: impl IntoIterator<Item = CaseId>,
        cases: Vec<CaseResult>,
    ) -> Self {
        Self {
            expected_cases: expected_cases.into_iter().collect(),
            cases,
        }
    }

    pub fn cases(&self) -> &[CaseResult] {
        &self.cases
    }

    pub fn expected_case_ids(&self) -> &[CaseId] {
        &self.expected_cases
    }

    pub fn verdict(&self, case_id: CaseId) -> Option<&CaseVerdict> {
        self.cases
            .iter()
            .find(|result| result.case_id == case_id)
            .map(|result| &result.verdict)
    }

    /// `Failed` and `NotProven` both make the report non-conformant.
    pub fn is_conformant(&self) -> bool {
        self.cases.len() == self.expected_cases.len()
            && self.expected_cases.iter().all(|case_id| {
                self.cases
                    .iter()
                    .filter(|result| result.case_id == *case_id)
                    .exactly_one()
                    .is_some_and(|result| result.verdict.is_passed())
            })
    }

    pub fn non_passing_case_ids(&self) -> impl Iterator<Item = CaseId> + '_ {
        self.cases
            .iter()
            .filter(|result| !result.verdict.is_passed())
            .map(|result| result.case_id)
    }
}

trait ExactlyOne: Iterator + Sized {
    fn exactly_one(mut self) -> Option<Self::Item> {
        let value = self.next()?;
        self.next().is_none().then_some(value)
    }
}

impl<T: Iterator> ExactlyOne for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_and_not_proven_are_both_non_conformant() {
        for verdict in [
            CaseVerdict::Failed {
                reason: "observed violation".to_owned(),
            },
            CaseVerdict::NotProven {
                reason: "missing positive evidence".to_owned(),
            },
        ] {
            let report = ConformanceReport::new(vec![CaseResult {
                case_id: case_ids::DESCRIPTOR_START_BINDING,
                verdict,
            }]);
            assert!(!report.is_conformant());
        }
    }

    #[test]
    fn a_partial_passed_report_is_not_conformant() {
        let report = ConformanceReport::new(vec![CaseResult {
            case_id: case_ids::DESCRIPTOR_START_BINDING,
            verdict: CaseVerdict::Passed,
        }]);

        assert!(!report.is_conformant());
    }
}
