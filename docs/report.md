# Verification results and production verdict

Verified locally on **13 November 2026**, on macOS arm64. Host API/worker binaries used Go 1.27.1; the container build used Go 1.24. PostgreSQL 17.12 ran in Docker with `synchronous_commit=on` and READ COMMITTED. Chaos processes ran on the host and connected to the container database; this was not a multi-host network test. Original outputs are preserved in [results](../results/README.md).

## Run count and outcomes

There were **three complete chaos runs of 10,000 jobs each**. Run 01 predates heartbeat renewal; runs 02 and 03 use the final service implementation from that development session. Run 01 was not replaced with newer evidence. After the first run, the harness's final key audit was made read-only.

Integration outputs 01–04 are preserved and all passed. Run 04 adds confirmation of actual row-lock waiting through pg_stat_activity. There was one load run, one Compose smoke run and one separate process-level zombie scenario. An earlier plain `go test ./...` without a database URL skipped database integration tests and is not evidence of database correctness. No failed chaos runs occurred in the recorded session.

| Run | Seed | SIGKILL events | Retry POSTs | Cancel requests | Succeeded | Failed | Cancelled | Effects | Jobs claimed again | Violations |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| chaos-01 | 42 | 21 | 10000 | 1429 | 8482 | 89 | 1429 | 8482 | 14 | 0 |
| chaos-02 | 99 | 20 | 10000 | 1429 | 8482 | 89 | 1429 | 8482 | 17 | 0 |
| chaos-03 | 777 | 20 | 10000 | 1429 | 8482 | 89 | 1429 | 8482 | 16 | 0 |

Elapsed times were 43.16, 41.41 and 40.47 seconds respectively. The 89 failed jobs in each run were **intentional simulated failures**, not lost jobs or failing tests. Nonzero `attempts > 1` counts show recovery through actual reclaims after worker termination. Each run retained exactly 10,000 expected keys with matching payloads and had no queued/running jobs or reported effect/state/token inconsistencies. The schema's primary key prevents duplicate effects. All three runs recorded zero HTTP scenario errors; process logs were also inspected and contained no ERROR messages. Ticker scheduling can introduce small variations in kill timing.

The harness submits jobs through 24 concurrent producers, repeats every POST in a separate goroutine and requests cancellation for every seventh job in another. Simulated work takes 15–45 ms. Kills continue during submission, processing and the final audit, with replacements started after termination. Consequently, kill totals include audit time after processing has finished. `attempts` counts claims, not committed effects. Cancel request counts need not generally equal final cancelled counts; completion can win the race.

## Silent failures and additional evidence

- **Double claim:** eight concurrent claim loops process 100 jobs, check unique claimed IDs and verify one effect per success. Row locks and SKIP LOCKED coordinate ownership of an attempt.
- **Zombie worker:** tests expire a lease and reject the old attempt before and after reclamation. The separate script also uses actual SIGSTOP, waits 2.3 seconds, starts a replacement, observes token 2, then resumes the original with SIGCONT. Result: succeeded, two attempts and one effect with the new token. Evidence: [zombie-01.log](../results/zombie-01.log).
- **Cancel/complete:** both deterministic orderings and 100 concurrent races are exercised. Cancellation first yields cancelled without an effect; completion first preserves success with an effect. The contract does not require cancellation always to win.
- **Request retry:** 50 concurrent creates sharing a key return one ID; a different payload returns 409. Chaos adds 10,000 retry POSTs per run.
- **Isolation:** claim, complete and renew explicitly use READ COMMITTED; cancellation is one atomic UPDATE. The setting and concurrency tests support this model. They do not compare every isolation level or establish equivalent behavior after arbitrary configuration changes.
- **Clock skew:** a client's lease value is changed to 1970 and to the future, but decisions still use SQL time. Another connection timezone is checked. Host clocks were not changed and no database-server clock jump was injected. SIGSTOP tests execution delay, not NTP failure.
- **Transaction boundary:** a trigger rejects the status update after the effect INSERT. The effect rolls back; completion succeeds after the trigger is removed. This is an expected error inside a passing test, not a discarded failing run.
- **Row-lock wait:** Complete is observed waiting in pg_stat_activity; the lease is expired under the held lock, and completion is rejected after obtaining it.
- **Renewal:** incorrect tokens, expired leases and cancelled jobs cannot renew. A 3.5-second job with a 2-second lease completes in one attempt through Compose. Simulated execution does not hold the claim transaction open.

[compose-smoke-01.log](../results/compose-smoke-01.log) also contains create/retry/status/cancel/list requests and SQL effect counts. [compose-up-02.log](../results/compose-up-02.log) records the successful container build and startup of the API and four workers. The vet logs are empty because go vet produced no diagnostics.

## 100 requests per second

[load-01/raw.log](../results/load-01/raw.log) records 3000 requests in 30.002 seconds: **99.99 requests/s**, zero request errors. A request is scheduled every 10 ms; overhead makes the measured average slightly lower than the target. The mix is 1500 job creations with 20 ms execution time and 1500 GETs of the first ten-item page.

P95 was **1.109 ms**, P99 **1.363 ms**, across all measured load-run HTTP requests. All 1500 jobs succeeded with 1500 effects, no remaining pending jobs, no reclaims and no reported invariant violations. Latency includes the HTTP round trip. In the opening log line, `jobs=10000` is the unused default chaos parameter; the actual load job count is recorded as `total=1500 expected=1500`.

These load observations differ from chaos latency samples, which include only initial creation requests, not retry or cancellation requests. The load mode reports rate and latency but does not enforce performance thresholds.

## Verdict: DON'T SHIP

**Do not promise production reliability at 100 requests/s on this evidence alone.** The tested configuration handled a short target-rate stream, but that does not establish operational reliability. A justified negative verdict is explicitly allowed by the assignment.

Fix and verify these areas first:

1. Bound SQL execution and lock-wait time, then test database unavailability, connection loss during COMMIT and recovery. A query against a stalled database can currently hold a worker until context cancellation; an API write timeout does not itself impose a SQL deadline.
2. Run sustained 100 requests/s with a write-heavy mix, cancellation, retries, long tasks and a large history. Define SLOs for HTTP latency and queue delay. Add metrics and alerts for queue depth, running-job age, retries, rejected completions, latency and errors.
3. Test PostgreSQL restart/failover, backup/restore and ambiguous COMMIT outcomes, then choose operational durability settings. The current database is a single instance without verified storage-failure recovery.

Before public access, the service also needs authentication, client isolation, managed secrets, admission limits and a retention policy for keys/history. If effects become external, add a publication/deduplication protocol such as a transactional outbox with an idempotent receiver. The present transaction guarantees a single committed effect only inside PostgreSQL.

## Harness limitations

The harness does not simulate network partitions, prolonged database degradation, disk loss, failover, database clock jumps, connection exhaustion, multiple API replicas, hours of computation, external effects, every cancellation/COMMIT interleaving or millions of historical rows. It uses the service's tables as its source of truth; a shared misunderstanding of business semantics can escape its checks.

Different seeds change SIGKILL victims but do not fully randomize business sequences. Rare interleavings may never occur. Each chaos retry follows the successful initial response for that job, so this scenario does not directly inject a lost initial HTTP response. The row primary key prevents duplicates, but only the shared transaction connects the effect to success; this is why a separate rollback test exists.

The subsequent code walkthrough identified additional limits in the current checker:

- A worker restart error is logged but is not propagated as a fatal run error; remaining workers could drain the queue and the run could still print PASS.
- The three-minute context is not a hard process-wide deadline. Readiness checks and load GETs use the HTTP client's timeout rather than the shared context, and context cancellation does not automatically interrupt arbitrary code.
- Cancel responses are not retained for per-job final-state assertions; final database consistency is checked separately.
- Achieved throughput and latency have no acceptance thresholds, so a slow load run may still pass its correctness checks.
- Some auxiliary errors are ignored, including the state-summary rows.Scan result.
- The API port is fixed at 18080, and readiness alone does not establish process identity. Keep it free and do not run multiple harness instances together.

These limitations were documented after review, not repaired as part of the English documentation update. Recorded PASS results refer to the implemented checks and must be interpreted with these limits.

The assignment specifies 10,000 jobs. That was enough to produce dozens of terminations and real reclaims locally, but is not statistical proof of correctness. At 10x load, likely pressure points are polling/transaction cost, database contention and queue growth; this is a hypothesis, not a measured 1000 requests/s result.

ID pagination is not a snapshot: a lower-ID insert committed after its page has been traversed can be missed. Snapshot traversal would need a separate protocol or snapshot mechanism.
