# Design (initial note written before implementation)

Revision: lease renewal was added after initial validation; see report.md for the preserved pre-renewal run.

Go API + independent Go worker processes + PostgreSQL. PostgreSQL is the source of truth for jobs and the simulated training effect. Unlike SQLite's single-writer model, row locks and SKIP LOCKED allow multiple workers and API transactions to make independent progress. PostgreSQL also provides server-side time, unique constraints and atomic multi-table commits; one database keeps the proof small. This is a design choice, not a claim that SQLite cannot implement a queue. The cost is another service to operate, connection management and database availability as a dependency.

## Claim and recovery

At READ COMMITTED a worker selects one queued job or expired running job using FOR UPDATE SKIP LOCKED. In the same transaction it sets running, increments a monotonically increasing fencing token, and sets a lease using PostgreSQL clock_timestamp(). Claim commits before simulated work. Every claim (including reclaim) consumes one attempt. After the attempt limit an expired job becomes failed. Workers poll; recovery needs at least one live worker and a reachable database. An expired lease is only evidence that the worker may be dead, not proof. No process-local clock is authoritative. A live worker renews its lease every lease/3 using DB time and the current token. An expired lease cannot be revived; a freeze or long database outage causes safe retries and eventual failure if the retry budget is exhausted.

## Complete and cancel

Completion locks the job and checks running, token and unexpired lease. It inserts the effect and updates succeeded in the SAME transaction. job_effects.job_id is a primary key. A zombie cannot commit against a replaced or expired token. Effects are exclusively database rows; arbitrary external files, emails or model training outputs do not inherit this guarantee.

Cancel locks/updates the same job row. Queued/running becomes cancelled immediately and the token increments. Simulated work may continue, but cannot commit an effect. If completion commits first, cancel returns the existing terminal result. If cancellation commits first, completion is rejected. Terminal states never change. Cancellation therefore means no future effect, not forceful termination of CPU work. Failed/cancelled jobs have zero committed effects; succeeded jobs have exactly one.

## API and guarantees

An idempotency key has a unique database constraint and stores the normalized validated payload. Concurrent matching requests return the same ID; reuse with a different payload returns 409. The key is retained indefinitely. An ambiguous HTTP response is retried with the same key. Lists use an increasing identity ID and keyset pagination (new insertions may appear on later pages).

Guarantee: at-least-once execution attempts subject to a finite retry budget, with exactly-once committed database effect for succeeded jobs. There is no promise every job succeeds: failures and cancellations are explicit terminal outcomes. A crash before commit rolls back effect and status together; after commit, a retry sees a terminal job. Database durability assumes PostgreSQL synchronous_commit=on and reliable storage. API and workers are stateless.

## Evidence plan and limits

Run deterministic integration tests for concurrent claims, retry deduplication, cancel/complete orders, expired and replaced tokens, rollback, retry exhaustion, isolation and client clock skew. Then run 10,000 jobs with eight OS worker processes, SIGKILL about every two seconds, concurrent duplicate POSTs and cancels; keep killing during drain and the final database audit, then stop and clean up the processes. Keep every attempted run's output. Measure 100 HTTP requests/s separately, including latency and errors. Chaos is sampling, not proof; deterministic tests exercise the critical boundaries. Process crashes do not emulate database failure, partitions, disk loss or failover.
