#!/usr/bin/env bash
# Local binaries connect to the Compose PostgreSQL. Each invocation uses fresh databases.
set -euo pipefail
cd "$(dirname "$0")/.."
run="$(date -u +%Y%m%dT%H%M%SZ)_$$"
mkdir -p results
exec > >(tee "results/verify-$run.log") 2>&1
trap 'echo "verify exit=$? at $(date -u +%FT%TZ)"' EXIT
docker compose up -d --wait db
go build -o bin/soup ./cmd/soup
go build -o bin/harness ./cmd/harness
go vet ./...
newdb() { docker compose exec -T db createdb -U soup "$1"; }
db="test_$run"
newdb "$db"
TEST_DATABASE_URL="postgres://soup:soup@localhost:54329/$db?sslmode=disable" go test -race -count=1 -v ./...
for seed in 42 99; do
  db="chaos_${run}_$seed"
  newdb "$db"
  DATABASE_URL="postgres://soup:soup@localhost:54329/$db?sslmode=disable" ./bin/harness -out "results/chaos-${run}-$seed" -seed "$seed"
done
db="load_$run"
newdb "$db"
DATABASE_URL="postgres://soup:soup@localhost:54329/$db?sslmode=disable" ./bin/harness -load -out "results/load-$run"
echo 'PASS: integration, two chaos runs, and load test'
# Databases/logs intentionally retained for audit; remove them explicitly when no longer needed.
