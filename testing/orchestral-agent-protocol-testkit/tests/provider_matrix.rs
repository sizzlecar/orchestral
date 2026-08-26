use std::collections::BTreeSet;

use orchestral_agent_protocol_testkit::{
    case_ids, CaseVerdict, ConformanceReport, ConformanceSuite, OpaqueAsyncNoRecoverFactory,
    OutcomeUnknownConformanceSuite, OutcomeUnknownFactory, ScriptedStatelessFactory,
    SessionfulRecoverFactory,
};

fn assert_full_pass(report: &ConformanceReport) {
    assert!(report.is_conformant(), "{:#?}", report.cases());
    assert_eq!(report.cases().len(), report.expected_case_ids().len());
    assert!(report
        .cases()
        .iter()
        .all(|result| matches!(result.verdict, CaseVerdict::Passed)));
}

#[tokio::test]
async fn scripted_stateless_provider_passes_negative_capability_branches() {
    let factory = ScriptedStatelessFactory::conformant().expect("valid scripted descriptor");
    let report = ConformanceSuite::run(&factory).await;

    assert_full_pass(&report);
    assert!(matches!(
        report.verdict(case_ids::RECOVER_UNSUPPORTED_WHEN_UNDECLARED),
        Some(CaseVerdict::Passed)
    ));
    assert!(matches!(
        report.verdict(case_ids::SESSION_CONFLICT_WHEN_REUSE_UNDECLARED),
        Some(CaseVerdict::Passed)
    ));
}

#[tokio::test]
async fn sessionful_provider_passes_recovery_and_100_run_isolation() {
    let factory = SessionfulRecoverFactory::new().expect("valid sessionful descriptor");
    let report = ConformanceSuite::run(&factory).await;

    assert_full_pass(&report);
    assert!(matches!(
        report.verdict(case_ids::RECOVER_STABLE_WHEN_DECLARED),
        Some(CaseVerdict::Passed)
    ));
    assert!(matches!(
        report.verdict(case_ids::SESSION_ISOLATION_WHEN_REUSE_DECLARED),
        Some(CaseVerdict::Passed)
    ));
}

#[tokio::test]
async fn opaque_async_provider_passes_without_recovery_escape_hatch() {
    let factory = OpaqueAsyncNoRecoverFactory::new().expect("valid opaque descriptor");
    let report = ConformanceSuite::run(&factory).await;

    assert_full_pass(&report);
    assert!(matches!(
        report.verdict(case_ids::RECOVER_UNSUPPORTED_WHEN_UNDECLARED),
        Some(CaseVerdict::Passed)
    ));
    assert!(matches!(
        report.verdict(case_ids::SESSION_CONFLICT_WHEN_REUSE_UNDECLARED),
        Some(CaseVerdict::Passed)
    ));
}

#[tokio::test]
async fn lost_first_start_response_passes_mandatory_1000_fault_suite() {
    let factory = OutcomeUnknownFactory::new().expect("valid fault descriptor");
    let report = OutcomeUnknownConformanceSuite::run(&factory).await;

    assert_full_pass(&report);
    assert_eq!(
        report.expected_case_ids(),
        &[case_ids::OUTCOME_UNKNOWN_IDENTITY_RETRY_1000]
    );
}

#[tokio::test]
async fn duplicate_work_adversary_only_fails_the_idempotency_case() {
    let factory = ScriptedStatelessFactory::duplicate_work().expect("valid adversarial descriptor");
    let report = ConformanceSuite::run(&factory).await;

    assert!(!report.is_conformant());
    assert_eq!(
        report.non_passing_case_ids().collect::<BTreeSet<_>>(),
        BTreeSet::from([case_ids::SAME_RUN_START_IDEMPOTENT])
    );
    assert!(matches!(
        report.verdict(case_ids::SAME_RUN_START_IDEMPOTENT),
        Some(CaseVerdict::Failed { .. })
    ));
}
