# Raw evidence index

Do not overwrite or edit raw outputs. Each harness directory is created exclusively and retains raw.log and processes.log.

- chaos-01: seed 42, 10k jobs; original implementation before heartbeat, PASS.
- chaos-02: seed 99, 10k jobs; final service with heartbeat, read-only final key audit, PASS.
- chaos-03: seed 777, same final service/harness, PASS.
- load-01: 3000 HTTP requests over 30s, 50/50 POST/GET, final service, PASS.
- *-console.log: unedited stdout/stderr of corresponding harness invocation.
- integration-01: original service, nine integration tests + Go race detector, PASS.
- integration-02: heartbeat and JSON null guard, ten tests + race detector, PASS.
- integration-03: final service, timezone/null test checks, PASS.
- integration-04: final tests including observed lock wait in pg_stat_activity, PASS.
- docker-build-01.log: first container build, before heartbeat, PASS.
- compose-up-02.log: final container build/start including four worker replicas, PASS.
- compose-smoke-01.log: actual HTTP requests and SQL effects, long job heartbeat, cancel/list, PASS.
- zombie-01.log: SIGSTOP -> expiry -> replacement -> SIGCONT using scripts/zombie_check.py, PASS.
- vet-01.log and vet-02.log: go vet successful; no diagnostics.

No failed chaos runs occurred in this work. Injected database errors in the rollback test are EXPECTED assertions, not discarded failures. See docs/report.md for counts, environment, interpretation, limitations and DON'T SHIP verdict. All results reflect this local environment only. source-sha256.txt identifies final source files, not the pre-heartbeat binary used by chaos-01.
