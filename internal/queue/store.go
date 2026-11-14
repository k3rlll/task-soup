package queue

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string
var ErrConflict = errors.New("idempotency key reused with different payload")
var ErrNotFound = errors.New("job not found")

type Store struct {
	Pool        *pgxpool.Pool
	Lease       time.Duration
	MaxAttempts int
}
type Payload struct {
	DurationMS      int  `json:"duration_ms"`
	SimulateFailure bool `json:"simulate_failure"`
}
type Job struct {
	ID  int64  `json:"id"`
	Key string `json:"idempotency_key"`
	Payload
	State      string     `json:"state"`
	Token      int64      `json:"token"`
	Attempts   int        `json:"attempts"`
	LeaseUntil *time.Time `json:"lease_until,omitempty"`
}

const columns = "id,idempotency_key,duration_ms,simulate_failure,state,token,attempts,lease_until"

func scan(r pgx.Row) (j Job, err error) {
	err = r.Scan(&j.ID, &j.Key, &j.DurationMS, &j.SimulateFailure, &j.State, &j.Token, &j.Attempts, &j.LeaseUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return
}
func Open(ctx context.Context, url string, lease time.Duration, maxAttempts int) (*Store, error) {
	if lease < 3*time.Millisecond || maxAttempts < 1 {
		return nil, fmt.Errorf("lease must be >=3ms and max attempts positive")
	}
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return &Store{p, lease, maxAttempts}, nil
}
func (s *Store) Migrate(ctx context.Context) error { _, err := s.Pool.Exec(ctx, schema); return err }
func (s *Store) Create(ctx context.Context, key string, p Payload) (Job, bool, error) {
	j, err := scan(s.Pool.QueryRow(ctx, `INSERT INTO jobs(idempotency_key,duration_ms,simulate_failure) VALUES($1,$2,$3) ON CONFLICT(idempotency_key) DO NOTHING RETURNING `+columns, key, p.DurationMS, p.SimulateFailure))
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return j, false, err
	}
	// A separate statement at READ COMMITTED sees the winner of a concurrent insert.
	j, err = scan(s.Pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE idempotency_key=$1`, key))
	if err == nil && j.Payload != p {
		err = ErrConflict
	}
	return j, false, err
}
func (s *Store) Get(ctx context.Context, id int64) (Job, error) {
	return scan(s.Pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1`, id))
}
func (s *Store) List(ctx context.Context, after int64, limit int) ([]Job, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+columns+` FROM jobs WHERE id>$1 ORDER BY id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []Job{}
	for rows.Next() {
		j, e := scan(rows)
		if e != nil {
			return nil, e
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
func (s *Store) Cancel(ctx context.Context, id int64) (Job, error) {
	return scan(s.Pool.QueryRow(ctx, `UPDATE jobs SET state=CASE WHEN state IN ('queued','running') THEN 'cancelled' ELSE state END, token=token+CASE WHEN state IN ('queued','running') THEN 1 ELSE 0 END, lease_until=NULL, updated_at=clock_timestamp() WHERE id=$1 RETURNING `+columns, id))
}
func (s *Store) Claim(ctx context.Context) (Job, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)
	j, err := scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE state='queued' OR (state='running' AND lease_until<=clock_timestamp()) ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1`))
	if err != nil {
		return j, err
	}
	state := "running"
	if j.Attempts >= s.MaxAttempts {
		state = "failed"
	}
	j, err = scan(tx.QueryRow(ctx, `UPDATE jobs SET state=$2,token=token+1,attempts=attempts+CASE WHEN $2='running' THEN 1 ELSE 0 END,lease_until=CASE WHEN $2='running' THEN clock_timestamp()+($3 * interval '1 millisecond') ELSE NULL END,updated_at=clock_timestamp() WHERE id=$1 RETURNING `+columns, j.ID, state, s.Lease.Milliseconds()))
	if err != nil {
		return j, err
	}
	return j, tx.Commit(ctx)
}
func (s *Store) Complete(ctx context.Context, j Job) (bool, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	// Lock first, then check time in a fresh statement, including time spent waiting for the lock.
	if _, err = scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1 FOR UPDATE`, j.ID)); err != nil {
		return false, err
	}
	var valid bool
	err = tx.QueryRow(ctx, `SELECT state='running' AND token=$2 AND lease_until>clock_timestamp() FROM jobs WHERE id=$1`, j.ID, j.Token).Scan(&valid)
	if err != nil || !valid {
		return false, err
	}
	state := "succeeded"
	if j.SimulateFailure {
		state = "failed"
	} else {
		if _, err = tx.Exec(ctx, `INSERT INTO job_effects(job_id,token) VALUES($1,$2)`, j.ID, j.Token); err != nil {
			return false, err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE jobs SET state=$2,lease_until=NULL,updated_at=clock_timestamp() WHERE id=$1`, j.ID, state)
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// Renew uses DB time after obtaining the row lock; an expired lease cannot revive.
func (s *Store) Renew(ctx context.Context, j Job) (bool, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if _, err = scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1 FOR UPDATE`, j.ID)); err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `UPDATE jobs SET lease_until=clock_timestamp()+($3*interval '1 millisecond'),updated_at=clock_timestamp() WHERE id=$1 AND token=$2 AND state='running' AND lease_until>clock_timestamp()`, j.ID, j.Token, s.Lease.Milliseconds())
	if err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
