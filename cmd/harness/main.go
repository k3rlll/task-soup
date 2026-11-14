package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/task-soup/queue/internal/queue"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", e)
		os.Exit(1)
	}
}
func run() (err error) {
	n := flag.Int(
		"jobs", 10000, "unique jobs",
	)

	seed := flag.Int64(
		"seed",
		42,
		"random seed",
	)

	bin := flag.String(
		"bin",
		"./bin/soup",
		"worker/API executable",
	)

	out := flag.String(
		"out",
		"",
		"NEW output directory",
	)

	load := flag.Bool(
		"load",
		false,
		"100 requests/sec for 30 seconds; no kills",
	)
	flag.Parse()

	if *out == "" || *n < 1 {
		return fmt.Errorf("-out required and jobs must be positive")
	}

	if e := os.Mkdir(*out, 0755); e != nil {
		return e
	}
	raw, e := os.Create(filepath.Join(*out, "raw.log"))
	if e != nil {
		return e
	}

	defer raw.Close()

	log := io.MultiWriter(os.Stdout, raw)
	start := time.Now()

	defer func() {
		fmt.Fprintf(log, "finished=%s elapsed=%s error=%v\n", time.Now().UTC().Format(time.RFC3339), time.Since(start), err)
	}()

	fmt.Fprintf(
		log,
		"started=%s seed=%d jobs=%d workers=8 kill_interval=2s load=%v\n",
		start.UTC().Format(time.RFC3339), *seed, *n, *load)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)

	defer cancel()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return fmt.Errorf("DATABASE_URL required: empty disposable database")
	}

	s, e := queue.Open(ctx, url, 2*time.Second, 10)
	if e != nil {
		return e
	}

	defer s.Pool.Close()
	if e = s.Migrate(ctx); e != nil {
		return e
	}

	var count int
	if e = s.Pool.QueryRow(
		ctx,
		"SELECT count(*) FROM jobs").Scan(&count); e != nil {
		return e
	}

	if count != 0 {
		return fmt.Errorf("database not empty: %d jobs; refusing to proceed", count)
	}

	childlog, e := os.Create(filepath.Join(*out, "processes.log"))
	if e != nil {
		return e
	}

	defer childlog.Close()

	spawn := func(mode string) (*exec.Cmd, error) {
		c := exec.Command(*bin, mode)
		c.Env = append(os.Environ(), "HTTP_ADDR=127.0.0.1:18080", "LEASE_DURATION=2s", "MAX_ATTEMPTS=10")
		c.Stdout = childlog
		c.Stderr = childlog
		return c, c.Start()
	}
	api, e := spawn("api")
	if e != nil {
		return e
	}

	defer func() { api.Process.Kill(); api.Wait() }()
	client := &http.Client{Timeout: 5 * time.Second}
	base := "http://127.0.0.1:18080"
	ready := false
	for i := 0; i < 100; i++ {
		r, e := client.Get(base + "/healthz")
		if e == nil {
			r.Body.Close()
			if r.StatusCode == 200 {
				ready = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !ready {
		return fmt.Errorf("API did not start")
	}
	workers := make([]*exec.Cmd, 8)
	defer func() {
		for _, c := range workers {
			if c != nil && c.Process != nil {
				c.Process.Kill()
				c.Wait()
			}
		}
	}()

	for i := range workers {
		workers[i], e = spawn("worker")
		if e != nil {
			return e
		}
	}

	request := func(method, path, key string, p *queue.Payload) (queue.Job, error) {
		var j queue.Job
		var b io.Reader
		if p != nil {
			data, _ := json.Marshal(p)
			b = bytes.NewReader(data)
		}

		req, e := http.NewRequestWithContext(ctx, method, base+path, b)
		if e != nil {
			return j, e
		}

		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		resp, e := client.Do(req)
		if e != nil {
			return j, e
		}

		defer resp.Body.Close()
		if resp.StatusCode != 200 && resp.StatusCode != 201 {
			data, _ := io.ReadAll(resp.Body)
			return j, fmt.Errorf("HTTP %d: %s", resp.StatusCode, data)
		}
		e = json.NewDecoder(resp.Body).Decode(&j)
		return j, e
	}

	var kills atomic.Int64
	stopKill := make(chan struct{})
	killDone := make(chan struct{})

	go func() {
		defer close(killDone)
		if *load {
			return
		}
		rng := rand.New(rand.NewSource(*seed))
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopKill:
				return
			case <-ticker.C:
				i := rng.Intn(8)
				c := workers[i]

				if e := c.Process.Kill(); e != nil {
					fmt.Fprintln(log, "kill error", e)
					continue
				}
				c.Wait()
				kills.Add(1)
				fmt.Fprintf(log, "kill elapsed=%s slot=%d pid=%d\n", time.Since(start).Round(time.Millisecond), i, c.Process.Pid)
				var e error
				workers[i], e = spawn("worker")

				if e != nil {
					fmt.Fprintln(log, "restart error", e)
					return
				}

			}
		}
	}()
	defer func() { close(stopKill); <-killDone }()
	var duplicates, cancels, errorsN atomic.Int64
	var samplesMu sync.Mutex
	latencies := []float64{}
	measure := func(f func() error) {
		t := time.Now()
		e := f()
		samplesMu.Lock()
		latencies = append(latencies, float64(time.Since(t).Microseconds())/1000)
		samplesMu.Unlock()
		if e != nil {
			errorsN.Add(1)
			fmt.Fprintln(log, "request error", e)
		}
	}
	expected := *n
	if *load {
		expected = 0
		var wg sync.WaitGroup
		ticker := time.NewTicker(10 * time.Millisecond)
		t0 := time.Now()
		for i := 0; i < 3000; i++ {
			<-ticker.C
			wg.Add(1)
			if i%2 == 0 {
				expected++
			}
			go func(i int) {
				defer wg.Done()
				measure(func() error {
					if i%2 == 0 {
						_, e := request("POST", "/jobs", fmt.Sprintf("load-%d", i), &queue.Payload{DurationMS: 20})
						return e
					}
					resp, e := client.Get(base + "/jobs?limit=10")
					if e != nil {
						return e
					}
					defer resp.Body.Close()
					io.Copy(io.Discard, resp.Body)
					if resp.StatusCode != 200 {
						return fmt.Errorf("list HTTP %d", resp.StatusCode)
					}
					return nil
				})
			}(i)
		}
		ticker.Stop()
		wg.Wait()
		fmt.Fprintf(log, "load requests=3000 target_rps=100 elapsed=%s achieved_rps=%.2f\n", time.Since(t0), 3000/time.Since(t0).Seconds())
	} else {
		ids := make([]atomic.Int64, *n)
		payload := func(i int) queue.Payload { return queue.Payload{DurationMS: 15 + i%31, SimulateFailure: i%97 == 0} }
		var wg sync.WaitGroup
		tasks := make(chan int)
		for k := 0; k < 24; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range tasks {
					i := i
					measure(func() error {
						p := payload(i)
						j, e := request("POST", "/jobs", fmt.Sprintf("chaos-%d", i), &p)
						if e == nil {
							ids[i].Store(j.ID)
						}
						return e
					})
				}
			}()
		}
		waitID := func(i int) int64 {
			for ids[i].Load() == 0 {
				if ctx.Err() != nil {
					return 0
				}
				time.Sleep(time.Millisecond)
			}
			return ids[i].Load()
		}
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < *n; i++ {
				id := waitID(i)
				if id == 0 {
					return
				}
				p := payload(i)
				j, e := request("POST", "/jobs", fmt.Sprintf("chaos-%d", i), &p)
				if e != nil || j.ID != id {
					errorsN.Add(1)
					fmt.Fprintln(log, "duplicate mismatch", i, j.ID, id, e)
				}
				duplicates.Add(1)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < *n; i += 7 {
				id := waitID(i)
				if id == 0 {
					return
				}
				_, e := request("POST", fmt.Sprintf("/jobs/%d/cancel", id), "", nil)
				if e != nil {
					errorsN.Add(1)
					fmt.Fprintln(log, "cancel error", e)
				}
				cancels.Add(1)
			}
		}()
		for i := 0; i < *n; i++ {
			tasks <- i
		}
		close(tasks)
		wg.Wait()
	}
	// Drain while chaos continues. Bound the wait so stuck running jobs fail the run.
	for {
		if ctx.Err() != nil {
			return fmt.Errorf("drain timeout: %w", ctx.Err())
		}
		var pending int
		if e = s.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE state IN ('queued','running')").Scan(&pending); e != nil {
			return e
		}
		fmt.Fprintf(log, "drain elapsed=%s pending=%d\n", time.Since(start).Round(time.Millisecond), pending)
		if pending == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	var total, bad, effects, attempts, retried int
	e = s.Pool.QueryRow(ctx, `SELECT count(*),coalesce(sum(attempts),0),count(*) FILTER (WHERE attempts>1) FROM jobs`).Scan(&total, &attempts, &retried)
	if e != nil {
		return e
	}
	e = s.Pool.QueryRow(ctx, `SELECT count(*) FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id WHERE j.state IN ('queued','running') OR (j.state='succeeded')<>(e.job_id IS NOT NULL) OR (e.job_id IS NOT NULL AND e.token<>j.token)`).Scan(&bad)
	if e != nil {
		return e
	}
	if e = s.Pool.QueryRow(ctx, "SELECT count(*) FROM job_effects").Scan(&effects); e != nil {
		return e
	}
	states := map[string]int{}
	rows, e := s.Pool.Query(ctx, "SELECT state,count(*) FROM jobs GROUP BY state")
	if e != nil {
		return e
	}
	for rows.Next() {
		var state string
		var c int
		rows.Scan(&state, &c)
		states[state] = c
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	// Check key/payload preservation too; total alone cannot show one missing and one extra job.
	keyMismatch := 0
	if !*load {
		for i := 0; i < *n; i++ {
			p := queue.Payload{DurationMS: 15 + i%31, SimulateFailure: i%97 == 0}
			var duration int
			var failure bool
			var state string
			e := s.Pool.QueryRow(ctx, "SELECT duration_ms,simulate_failure,state FROM jobs WHERE idempotency_key=$1", fmt.Sprintf("chaos-%d", i)).Scan(&duration, &failure, &state)
			if e != nil || duration != p.DurationMS || failure != p.SimulateFailure || state == "queued" || state == "running" {
				keyMismatch++
			}
		}
	}
	sort.Float64s(latencies)
	p95 := 0.0
	p99 := 0.0
	if len(latencies) > 0 {
		p95 = latencies[(len(latencies)-1)*95/100]
		p99 = latencies[(len(latencies)-1)*99/100]
	}
	fmt.Fprintf(log, "SUMMARY total=%d expected=%d states=%v effects=%d attempts=%d retried=%d kills=%d duplicates=%d cancellations=%d violations=%d key_mismatches=%d request_errors=%d latency_p95_ms=%.3f latency_p99_ms=%.3f\n", total, expected, states, effects, attempts, retried, kills.Load(), duplicates.Load(), cancels.Load(), bad, keyMismatch, errorsN.Load(), p95, p99)
	if total != expected || bad != 0 || keyMismatch != 0 || errorsN.Load() != 0 {
		return fmt.Errorf("invariants or requests failed")
	}
	if !*load && kills.Load() == 0 {
		return fmt.Errorf("no worker was killed")
	}
	fmt.Fprintln(log, "PASS")
	return nil
}
