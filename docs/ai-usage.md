# Human and AI contribution record

## Candidate contribution

I have selected PostgreSQL. Interpreted the assignment, proposed the detailed architecture, wrote the initial design note before implementation, and wrote the API, worker and PostgreSQL code. The architecture proposed by me uses row locks and SKIP LOCKED for claims, leases and fencing tokens for ownership, a database uniqueness constraint for request idempotency, and a shared transaction for the effect and terminal status.

## AI contribution

It performed the routine implementation support: generating integration tests, the chaos/load harness, Docker configuration, verification scripts, raw evidence summaries and documentation. AI executed builds, go vet, database integration tests with the Go race detector, container startup, chaos runs and a limited load test. Original outputs are retained in [results](../results/README.md). AI also generated the README files.


