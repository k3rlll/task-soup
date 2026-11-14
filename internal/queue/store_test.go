package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL required; use a dedicated disposable database")
	}
	s, e := Open(context.Background(), url, 2*time.Second, 3)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Pool.Close)
	if e = s.Migrate(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Pool.Exec(context.Background(), "TRUNCATE job_effects,jobs RESTART IDENTITY"); e != nil {
		t.Fatal(e)
	}
	return s
}
func create(t *testing.T, s *Store, key string) Job {
	t.Helper()
	j, _, e := s.Create(context.Background(), key, Payload{})
	if e != nil {
		t.Fatal(e)
	}
	return j
}
func claim(t *testing.T, s *Store) Job {
	t.Helper()
	j, e := s.Claim(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	return j
}
func complete(t *testing.T, s *Store, j Job, want bool) {
	t.Helper()
	ok, e := s.Complete(context.Background(), j)
	if e != nil || ok != want {
		t.Fatalf("completion=%v err=%v want=%v", ok, e, want)
	}
}
func invariants(t *testing.T, s *Store) {
	t.Helper()
	var n int
	err := s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id WHERE (j.state='succeeded') <> (e.job_id IS NOT NULL) OR (e.job_id IS NOT NULL AND e.token<>j.token)`).Scan(&n)
	if err != nil || n != 0 {
		t.Fatalf("invariant violations=%d err=%v", n, err)
	}
}
func TestConcurrentIdempotency(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	ids := make(chan int64, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, _, e := s.Create(ctx, "same", Payload{})
			if e != nil {
				t.Error(e)
				return
			}
			ids <- j.ID
		}()
	}
	wg.Wait()
	close(ids)
	var first int64
	for id := range ids {
		if first == 0 {
			first = id
		}
		if id != first {
			t.Fatal("duplicate job")
		}
	}
	if _, _, e := s.Create(ctx, "same", Payload{DurationMS: 1}); !errors.Is(e, ErrConflict) {
		t.Fatalf("expected conflict: %v", e)
	}
}
func TestConcurrentClaim(t *testing.T) {
	s := testStore(t)
	for i := 0; i < 100; i++ {
		create(t, s, fmt.Sprint(i))
	}
	var wg sync.WaitGroup
	seen := sync.Map{}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, e := s.Claim(context.Background())
				if errors.Is(e, ErrNotFound) {
					return
				}
				if e != nil {
					t.Error(e)
					return
				}
				if _, dup := seen.LoadOrStore(j.ID, true); dup {
					t.Error("double claim", j.ID)
				}
				complete(t, s, j, true)
			}
		}()
	}
	wg.Wait()
	invariants(t, s)
	var n int
	s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM job_effects").Scan(&n)
	if n != 100 {
		t.Fatal(n)
	}
}
func TestZombieAndExpiredLease(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	create(t, s, "zombie")
	old := claim(t, s)
	if _, e := s.Pool.Exec(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", old.ID); e != nil {
		t.Fatal(e)
	}
	complete(t, s, old, false) // Expired even before a replacement exists.
	replacement := claim(t, s)
	if replacement.Token <= old.Token {
		t.Fatal("token did not advance")
	}
	complete(t, s, old, false)
	complete(t, s, replacement, true)
	complete(t, s, replacement, false)
	invariants(t, s)
}
func TestCancelCompleteOrdersAndRace(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	create(t, s, "cancel-first")
	j := claim(t, s)
	c, e := s.Cancel(ctx, j.ID)
	if e != nil || c.State != "cancelled" {
		t.Fatal(c, e)
	}
	complete(t, s, j, false)
	create(t, s, "complete-first")
	j = claim(t, s)
	complete(t, s, j, true)
	c, e = s.Cancel(ctx, j.ID)
	if e != nil || c.State != "succeeded" {
		t.Fatal(c, e)
	}
	for i := 0; i < 100; i++ {
		create(t, s, fmt.Sprint(i))
		j = claim(t, s)
		var wg sync.WaitGroup
		wg.Add(2)
		go func(j Job) {
			defer wg.Done()
			if _, e := s.Complete(ctx, j); e != nil {
				t.Error(e)
			}
		}(j)
		go func(j Job) {
			defer wg.Done()
			if _, e := s.Cancel(ctx, j.ID); e != nil {
				t.Error(e)
			}
		}(j)
		wg.Wait()
	}
	invariants(t, s)
}
func TestAttemptExhaustionAndFailure(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	create(t, s, "exhaust")
	for i := 0; i < 3; i++ {
		j := claim(t, s)
		if j.Attempts != i+1 {
			t.Fatal(j)
		}
		s.Pool.Exec(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID)
	}
	j := claim(t, s)
	if j.State != "failed" || j.Attempts != 3 {
		t.Fatal(j)
	}
	j, _, e := s.Create(ctx, "failure", Payload{SimulateFailure: true})
	if e != nil {
		t.Fatal(e)
	}
	j = claim(t, s)
	complete(t, s, j, true)
	j, e = s.Get(ctx, j.ID)
	if e != nil || j.State != "failed" {
		t.Fatal(j, e)
	}
	invariants(t, s)
}
func TestAtomicRollback(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	create(t, s, "rollback")
	j := claim(t, s)
	// Fail status update AFTER the effect INSERT. Completion must roll back both.
	_, e := s.Pool.Exec(ctx, `CREATE OR REPLACE FUNCTION reject_success() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='succeeded' THEN RAISE EXCEPTION 'injected failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_success BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION reject_success()`)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		s.Pool.Exec(ctx, "DROP TRIGGER IF EXISTS reject_success ON jobs; DROP FUNCTION IF EXISTS reject_success()")
	})
	if _, e = s.Complete(ctx, j); e == nil {
		t.Fatal("expected injected failure")
	}
	invariants(t, s)
	var n int
	s.Pool.QueryRow(ctx, "SELECT count(*) FROM job_effects").Scan(&n)
	if n != 0 {
		t.Fatal("effect survived rollback")
	}
	s.Pool.Exec(ctx, "DROP TRIGGER reject_success ON jobs")
	complete(t, s, j, true)
}
func TestDatabaseTimeAndIsolation(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	var iso string
	if e := s.Pool.QueryRow(ctx, "SHOW transaction_isolation").Scan(&iso); e != nil || iso != "read committed" {
		t.Fatal(iso, e)
	}
	create(t, s, "clock")
	j := claim(t, s)
	// Simulate a wildly skewed client timestamp. Complete must consult DB, not this field.
	past := time.Unix(0, 0)
	j.LeaseUntil = &past
	complete(t, s, j, true)
	create(t, s, "future")
	j = claim(t, s)
	s.Pool.Exec(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID)
	future := time.Now().Add(24 * time.Hour)
	j.LeaseUntil = &future
	complete(t, s, j, false)
	// A timezone difference changes display only, not lease comparisons.
	conn, e := s.Pool.Acquire(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Release()
	if _, e = conn.Exec(ctx, "SET TIME ZONE 'Pacific/Honolulu'"); e != nil {
		t.Fatal(e)
	}
	defer conn.Exec(ctx, "RESET TIME ZONE")
	var positive bool
	if e = conn.QueryRow(ctx, "SELECT clock_timestamp() < clock_timestamp()+interval '1 second'").Scan(&positive); e != nil || !positive {
		t.Fatal(e)
	}
}
func TestHTTPContract(t *testing.T) {
	s := testStore(t)
	h := Handler(s)
	request := func(method, path, key, body string, want int) []byte {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		return w.Body.Bytes()
	}
	request("POST", "/jobs", "", "{}", 400)
	request("POST", "/jobs", "null", "null", 400)
	request("POST", "/jobs", "bad", "{\"duration_ms\":-1}", 400)
	request("POST", "/jobs", "bad", "{} {}", 400)
	var j Job
	json.Unmarshal(request("POST", "/jobs", "key", "{}", 201), &j)
	request("POST", "/jobs", "key", "{}", 200)
	request("POST", "/jobs", "key", "{\"duration_ms\":1}", 409)
	request("GET", fmt.Sprintf("/jobs/%d", j.ID), "", "", 200)
	request("POST", fmt.Sprintf("/jobs/%d/cancel", j.ID), "", "", 200)
	request("GET", "/jobs/99999", "", "", 404)
	for i := 0; i < 5; i++ {
		create(t, s, fmt.Sprint(i))
	}
	var page struct {
		Jobs []Job `json:"jobs"`
		Next int64 `json:"next_after"`
	}
	var after int64
	seen := map[int64]bool{}
	for {
		json.Unmarshal(request(http.MethodGet, fmt.Sprintf("/jobs?limit=2&after=%d", after), "", "", 200), &page)
		if len(page.Jobs) == 0 {
			break
		}
		for _, j := range page.Jobs {
			if seen[j.ID] || j.ID <= after {
				t.Fatal("pagination duplicate/order")
			}
			seen[j.ID] = true
		}
		after = page.Next
	}
	if len(seen) != 6 {
		t.Fatal(len(seen))
	}
	request("GET", "/jobs?limit=0", "", "", 400)
}

func TestLeaseExpiresWhileWaitingForLock(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	create(t, s, "wait-lock")
	j := claim(t, s)
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", j.ID); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		ok, e := s.Complete(ctx, j)
		if e == nil && ok {
			e = fmt.Errorf("expired completion accepted")
		}
		done <- e
	}()
	deadline := time.Now().Add(time.Second)
	for {
		var waiting bool
		err := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%FROM jobs WHERE id=$1 FOR UPDATE%')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completion did not block on the held row lock")
		}
		time.Sleep(time.Millisecond)
	}
	// Expire under the held lock. Completion must check DB time AFTER acquiring it.
	if _, e = tx.Exec(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	invariants(t, s)
}

func TestLeaseRenewal(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	create(t, s, "renew")
	j := claim(t, s)
	if ok, e := s.Renew(ctx, j); e != nil || !ok {
		t.Fatal(ok, e)
	}
	bad := j
	bad.Token++
	if ok, e := s.Renew(ctx, bad); e != nil || ok {
		t.Fatal(ok, e)
	}
	s.Pool.Exec(ctx, "UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", j.ID)
	if ok, e := s.Renew(ctx, j); e != nil || ok {
		t.Fatal("expired lease revived", ok, e)
	}
	newer := claim(t, s)
	if ok, e := s.Renew(ctx, j); e != nil || ok {
		t.Fatal("stale lease revived", ok, e)
	}
	s.Cancel(ctx, newer.ID)
	if ok, e := s.Renew(ctx, newer); e != nil || ok {
		t.Fatal("cancelled lease revived", ok, e)
	}
}
