# Soup — fault-tolerant training-job queue

A Go HTTP API and independent workers backed by PostgreSQL. Clients create, inspect, list and cancel simulated training jobs. Workers recover abandoned jobs after crashes and commit a single database effect for each successful job.

**Review status:** three recorded 10,000-job chaos runs passed their implemented checks. The production verdict remains **DON'T SHIP**: short local runs do not establish reliability during database failures or sustained production traffic. See the [results and limitations](docs/report.md).


## Start here

1. Follow [Quick start](#quick-start) to launch the API and four workers.
2. Read the compact [design note](docs/design.md) for ownership, recovery and consistency guarantees.
3. Run [Verification](#verification) and inspect the preserved [raw evidence](results/README.md).
4. Review the [requirement mapping](docs/requirements.md) and [production verdict](docs/report.md).
5. Use the [repository guide](#repository-guide) below to navigate the implementation.
6. See the AI usage [report](docs/ai-usage.md).

## Quick start

**Required:** Docker with Docker Compose v2. Run commands from the repository root.

```sh
docker compose up --build
```

Compose waits for PostgreSQL to become healthy, runs the initial migration, and starts the API plus **four worker processes in separate containers**. On subsequent starts, `docker compose up` is sufficient unless the image needs rebuilding.

| Component | Local address |
|---|---|
| HTTP API | `http://localhost:8080` |
| Readiness endpoint | `http://localhost:8080/healthz` |
| PostgreSQL | `localhost:54329`, database/user/password: `soup` |

In another terminal:

```sh
curl -sS http://localhost:8080/healthz

curl -i -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: training-001' \
  -d '{"duration_ms":100,"simulate_failure":false}'
```

The creation response contains the job's `id`. Substitute that value for `JOB_ID` below; identity values can have gaps and should not be assumed to start at a particular value in an existing database.

```sh
JOB_ID=1  # Replace with the ID returned by POST /jobs.
curl -sS "http://localhost:8080/jobs/$JOB_ID"
curl -sS 'http://localhost:8080/jobs?after=0&limit=20'
curl -sS -X POST "http://localhost:8080/jobs/$JOB_ID/cancel"
```

A short job may finish before cancellation arrives. In that case cancel returns its existing terminal status. Repeat the creation request with the same key and payload to observe idempotency: the ID remains the same even if the status has changed.

```sh
docker compose ps
docker compose logs api worker
docker compose down
```

`down` stops the deployment and preserves the database volume. `docker compose down -v` also **deletes the stored data**. The test harness does not run that command. Ports are bound to localhost and the demo credentials are intended for local use.

## API reference

Responses are JSON. Job objects expose `id`, `idempotency_key`, `duration_ms`, `simulate_failure`, `state`, `token`, `attempts`, and `lease_until` when present. The token and attempts fields make ownership and recovery visible during testing.

| Method | Route | Behavior |
|---|---|---|
| POST | `/jobs` | Create with an `Idempotency-Key` header: 201 for a new job, 200 for a matching retry, 409 for a conflicting payload. |
| GET | `/jobs/{id}` | Return the current job, or 404 if absent. |
| GET | `/jobs?after=0&limit=100` | Return `jobs` and `next_after`, ordered by increasing ID. |
| POST | `/jobs/{id}/cancel` | Cancel queued/running work; return an existing terminal result unchanged in state. Missing ID: 404. |
| GET | `/healthz` | Ping PostgreSQL: 200 when reachable, 503 otherwise. This does not check worker health. |

### Creation and retries

The key is required, nonblank, and at most 200 bytes. Keys are retained indefinitely. A retry must contain the same **normalized payload**: omitted fields and their zero-value equivalents are treated alike. Reusing a key with a different payload returns 409.

The body must be one JSON object, up to 4096 bytes. Unknown fields, `null`, trailing JSON values and invalid field values are rejected with 400.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `duration_ms` | integer | `0` | Simulated execution time, from 0 through 30000 ms. |
| `simulate_failure` | boolean | `false` | End the owned attempt as `failed`, without inserting an effect. |

This service simulates training. Its observable side effect is a row in `job_effects`; it does not execute an actual machine-learning workload.

### Pagination

`after` is a nonnegative ID cursor. `limit` accepts 1–1000, default 100. Pass `next_after` into the next request until `jobs` is empty. Invalid parameters return 400.

This is keyset pagination, not a consistent snapshot. New commits can appear on later pages, and an insert with a lower allocated ID that commits after that page was read can be missed during a concurrent traversal.

### Cancellation

Queued or running jobs become `cancelled`. Cancellation and completion serialize on the same row: the first committed transition determines the terminal outcome. If completion wins, cancel returns `succeeded` or `failed`; if cancel wins, completion cannot insert an effect. Repeated cancellation is safe. The simulated computation may continue briefly, but ownership checks prevent a later committed result.

## Architecture and guarantees

```text
Clients -> Go HTTP API -> PostgreSQL: jobs
                               ^
                               |
                    independent Go workers
                      claim / renew / complete
                               |
                               v
                     PostgreSQL: job_effects
```

The API and workers share PostgreSQL as the source of truth. There is no separate message broker and no in-memory queue whose loss would discard accepted jobs.

1. **Create:** a unique constraint on `idempotency_key` deduplicates concurrent requests. A conflicting insert is followed by a separate read to observe the winning committed row.
2. **Claim:** at READ COMMITTED, `FOR UPDATE SKIP LOCKED` selects an available queued job or an expired running job. The transaction increments its attempt count and fencing token and assigns a lease using database time.
3. **Execute:** work happens outside the transaction. A live worker renews the lease every `lease/3`; renewal requires an unexpired lease and the current token.
4. **Recover:** after a worker dies or freezes, another worker can reclaim its expired job with a new token. After the retry budget is exhausted, an expired job becomes `failed`.
5. **Complete:** the worker locks the row, checks state, token and database time, and inserts the effect together with the `succeeded` transition in one transaction. A simulated failure commits `failed` without an effect.
6. **Cancel:** the API updates the same row and invalidates ownership. A resumed old worker cannot publish a stale result.

State transitions are `queued -> running -> succeeded / failed / cancelled`, with direct cancellation from `queued` and reclamation of expired `running` jobs. Terminal states remain terminal.

**Guarantee:** repeatable execution attempts subject to a finite retry budget, with exactly one committed database effect for each `succeeded` job and none for `failed` or `cancelled`. `attempts` counts claims, not committed effects. External files, emails and real training outputs require their own publication/deduplication protocol.

### Why PostgreSQL

- Row locks and `SKIP LOCKED` let several workers claim different jobs concurrently.
- A single transaction commits the effect and terminal success together. `PRIMARY KEY(job_id)` additionally rejects a second effect.
- A unique idempotency key handles concurrent request retries in the database.
- Server-side time provides one authority for lease decisions, while fencing tokens reject stale workers.

SQLite can implement a queue, but its single-writer model makes PostgreSQL a more direct fit for this concurrent workload. This is an architectural rationale, not a measured SQLite throughput limit. PostgreSQL adds operational costs: a separate service, connection management, backups and database availability. Jumps in the database server's own clock remain a risk.

## Configuration and local execution

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgres://soup:soup@localhost:54329/soup?sslmode=disable` | PostgreSQL connection string; Compose overrides the host to `db:5432`. |
| `HTTP_ADDR` | `:8080` | API listening address. |
| `LEASE_DURATION` | `2s` | Ownership timeout; validated minimum 3 ms, but practical values need margin for DB and scheduler delays. |
| `MAX_ATTEMPTS` | `10` | Maximum number of claims before expired work is marked failed. |

To run the Go binary on the host, install Go 1.24+ and start only the database:

```sh
docker compose up -d --wait db
go build -o bin/soup ./cmd/soup
./bin/soup migrate
./bin/soup api
# In separate terminals, start the desired number of workers:
./bin/soup worker
```

If the Compose API is already using port 8080, stop it first or choose another `HTTP_ADDR`. Migration must precede API/worker use. The initial schema creation is idempotent; production schema evolution would need versioned migrations. The runtime container uses a non-root user.

## Verification

Local verification requires Go 1.24+, Docker Compose, Bash and a Unix host for process signals. The race detector also needs a working C toolchain. Python 3 is needed only for the separate zombie scenario.

```sh
./scripts/verify.sh
```

The script starts the database, builds both binaries, runs `go vet`, runs integration tests with the race detector, performs two 10,000-job chaos runs, and runs the short load test. Each stage uses a fresh dedicated database. Logs and test databases are retained for inspection. It does not clear the application database `soup`.

The harness uses port **18080**. Keep it free and run one harness at a time. Its workers run on the host and connect to Docker PostgreSQL; they are separate from the four Compose workers connected to `soup`.

### Integration tests only

```sh
docker compose up -d --wait db
docker compose exec -T db createdb -U soup soup_test_manual
TEST_DATABASE_URL='postgres://soup:soup@localhost:54329/soup_test_manual?sslmode=disable' \
  go test -race -count=1 -v ./...
```

Tests truncate tables in the selected database. Always use a dedicated test database. Without `TEST_DATABASE_URL`, database integration tests are explicitly skipped: plain `go test ./...` is not equivalent to the full check.

Tests cover concurrent claims, concurrent idempotent creation, conflicting payloads, both cancellation/completion orders and concurrent races, expired/stale ownership, retry exhaustion, rollback after effect insertion, database-time checks, HTTP validation, pagination and lease renewal.

### Chaos harness

```sh
go build -o bin/soup ./cmd/soup
go build -o bin/harness ./cmd/harness
docker compose exec -T db createdb -U soup soup_chaos_manual
DATABASE_URL='postgres://soup:soup@localhost:54329/soup_chaos_manual?sslmode=disable' \
  ./bin/harness -out results/chaos-manual-01 -seed 42
```

Use a new empty database and a new output directory for every run; choose unused names if repeating these commands. The harness starts an API and eight OS worker processes, submits 10,000 jobs, repeats every POST and requests cancellation for every seventh job. It kills a randomly selected worker with SIGKILL approximately every two seconds and starts a replacement.

It drains the queue and checks total count, original keys/payloads, terminal states, effect presence and effect tokens. `raw.log` holds progress and assertions; `processes.log` holds API/worker output. Preserve both files, including failed runs. Flags: `-jobs` (default 10000), `-seed` (42), `-bin` (`./bin/soup`), required `-out`, and `-load`.

### Load test

```sh
docker compose exec -T db createdb -U soup soup_load_manual
DATABASE_URL='postgres://soup:soup@localhost:54329/soup_load_manual?sslmode=disable' \
  ./bin/harness -load -out results/load-manual-01
```

Load mode schedules 3000 requests at a target 100 requests/s: 1500 creates and 1500 list requests. It does not kill workers. It reports achieved throughput and latency, then checks completion and database invariants. Throughput and latency are observations, not enforced pass/fail thresholds. See [harness limitations](docs/report.md#harness-limitations) before interpreting PASS.

### Real zombie-worker scenario

```sh
docker compose exec -T db createdb -U soup soup_zombie_manual
DATABASE_URL='postgres://soup:soup@localhost:54329/soup_zombie_manual?sslmode=disable' \
  ZOMBIE_DB=soup_zombie_manual python3 -u scripts/zombie_check.py
```

This script uses port **18081**, pauses a worker with SIGSTOP, waits for expiry, starts a replacement, resumes the original with SIGCONT, and verifies one effect from the new token. It requires the built `bin/soup` and Python's standard library. It is a separate check, not a step inside `verify.sh`.

## Recorded results

| Check | Recorded outcome |
|---|---|
| Chaos | Three runs of 10,000 jobs; 61 SIGKILL events in total; zero reported invariant violations. |
| Final service chaos runs | Runs 02 and 03 include heartbeat; run 01 preserves the earlier implementation's evidence. |
| Load | 3000 requests in 30.002 s; 99.99 requests/s; no request errors; P95 1.109 ms, P99 1.363 ms locally. |
| Integration | Four recorded passes as checks evolved, including `go test -race`. |
| Process scenarios | Compose smoke and SIGSTOP/SIGCONT recovery passed. |

The [full report](docs/report.md) explains intentional failed jobs, remaining gaps and the **DON'T SHIP** verdict. [results/README.md](results/README.md) indexes the unmodified evidence.

## Repository guide

```text
.
├── cmd/
│   ├── soup/main.go          # Service entry point: migrate, api, worker
│   └── harness/main.go       # Chaos/load runner and final invariant audit
├── internal/queue/
│   ├── http.go               # Routes, validation and JSON responses
│   ├── store.go              # PostgreSQL operations and ownership protocol
│   ├── schema.sql            # Tables, constraints and claim index
│   └── store_test.go         # Database integration and HTTP contract tests
├── scripts/
│   ├── verify.sh             # Repeatable build/test/chaos/load workflow
│   └── zombie_check.py       # Actual SIGSTOP/SIGCONT recovery scenario
├── docs/
│   ├── design.md             # Compact architecture and guarantee note
│   ├── requirements.md       # Assignment-to-evidence mapping
│   ├── report.md             # Measurements, limitations and verdict
│   └── ai-usage.md           # Human/AI contribution record
├── results/                  # Preserved raw runs, process logs and source hashes
├── compose.yaml              # PostgreSQL, migration, API and four workers
├── Dockerfile                # Multi-stage Go build and non-root runtime
├── go.mod / go.sum           # Module requirements and dependency checksums
├── .dockerignore             # Excludes local metadata, binaries and logs from builds
├── .gitignore                # Excludes editor files, local binaries and .env
└── LICENSE                   # Repository license
```

### Service entry point: `cmd/soup`

Parses environment configuration, opens the database and selects one of three modes. `migrate` installs the embedded schema. `api` serves HTTP with request I/O timeouts and signal-based shutdown. `worker` repeatedly claims jobs, simulates execution with a timer, renews ownership and attempts completion. Claim errors back off through a short polling delay; losing ownership prevents publication.

### Queue implementation: `internal/queue`

`http.go` translates HTTP requests into store calls and maps expected errors to status codes. `store.go` holds the consistency rules: `Create`, `Get`, `List`, `Cancel`, `Claim`, `Renew` and `Complete`. `schema.sql` defines `jobs`, unique idempotency keys, the partial claim index and `job_effects` with a primary/foreign key on `job_id`. `store_test.go` exercises these rules against real PostgreSQL; it also uses the HTTP handler directly for contract tests.

### Test driver: `cmd/harness`

Starts and cleans up child processes, drives concurrent requests, injects process crashes and queries PostgreSQL for final invariants. Twenty-four producer goroutines issue initial creates; separate goroutines issue retries and cancellations. A seeded random generator selects worker slots for termination. Load mode uses a separate fixed-rate request schedule. This executable checks the service; it is not part of the deployed API.

### Automation: `scripts`

`verify.sh` creates uniquely named test databases and retains a combined execution log plus harness output directories. `zombie_check.py` isolates a paused-worker scenario that ordinary SIGKILL does not exercise: an old process actually resumes after its job has been reassigned.

### Documentation and evidence: `docs` and `results`

`docs` explains decisions, claims and limits. `results` contains observations from actual runs; logs should not be rewritten to match a conclusion. `source-sha256.txt` identifies the final source files at the time of the recorded runs and does not identify the earlier pre-heartbeat binary. The ignored `bin/` directory is generated locally by `go build` and is not required in the source submission.
