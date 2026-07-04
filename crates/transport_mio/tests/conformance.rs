//! Runs the shared `transport_core::testing::run_conformance_suite` against
//! `MioTransport`. The suite auto-spins a `127.0.0.1:0` listener for
//! `connect_tcp` so UDP + TCP both close the loop end-to-end.

use transport_core::testing::run_conformance_suite;
use transport_mio::MioTransport;

#[tokio::test]
async fn conformance_suite_all_cases_pass() {
    let report = run_conformance_suite::<MioTransport>().await;
    assert!(report.all_passed(), "conformance suite failed: {report:?}");
    assert!(report.passed.contains(&"bind_udp"));
    assert!(report.passed.contains(&"name_non_empty"));
    assert!(report.passed.contains(&"connect_tcp"));
}
