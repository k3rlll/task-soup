package queue

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

func Handler(s *Store) http.Handler {
	m := http.NewServeMux()
	reply := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(v)
	}
	fail := func(w http.ResponseWriter, err error) {
		code := 500
		msg := "internal error"
		if errors.Is(err, ErrNotFound) {
			code = 404
			msg = err.Error()
		}
		if errors.Is(err, ErrConflict) {
			code = 409
			msg = err.Error()
		}
		if code == 500 {
			slog.Error("request failed", "error", err)
		}
		reply(w, code, map[string]string{"error": msg})
	}
	id := func(w http.ResponseWriter, r *http.Request) (int64, bool) {
		n, e := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if e != nil || n < 1 {
			reply(w, 400, map[string]string{"error": "invalid job ID"})
			return 0, false
		}
		return n, true
	}
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Pool.Ping(r.Context()); err != nil {
			reply(w, 503, map[string]string{"status": "unavailable"})
			return
		}
		reply(w, 200, map[string]string{"status": "ok"})
	})
	m.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if strings.TrimSpace(key) == "" || len(key) > 200 {
			reply(w, 400, map[string]string{"error": "Idempotency-Key must be 1..200 bytes and nonblank"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		var p *Payload
		if err := d.Decode(&p); err != nil || p == nil {
			reply(w, 400, map[string]string{"error": "invalid JSON"})
			return
		}
		if err := d.Decode(new(any)); err != io.EOF {
			reply(w, 400, map[string]string{"error": "expected one JSON object"})
			return
		}
		if p.DurationMS < 0 || p.DurationMS > 30000 {
			reply(w, 400, map[string]string{"error": "duration_ms must be 0..30000"})
			return
		}
		j, created, err := s.Create(r.Context(), key, *p)
		if err != nil {
			fail(w, err)
			return
		}
		code := 200
		if created {
			code = 201
		}
		reply(w, code, j)
	})
	m.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		n, ok := id(w, r)
		if !ok {
			return
		}
		j, err := s.Get(r.Context(), n)
		if err != nil {
			fail(w, err)
			return
		}
		reply(w, 200, j)
	})
	m.HandleFunc("POST /jobs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		n, ok := id(w, r)
		if !ok {
			return
		}
		j, err := s.Cancel(r.Context(), n)
		if err != nil {
			fail(w, err)
			return
		}
		reply(w, 200, j)
	})
	m.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) {
		after := int64(0)
		limit := 100
		var err error
		if v := r.URL.Query().Get("after"); v != "" {
			after, err = strconv.ParseInt(v, 10, 64)
			if err != nil || after < 0 {
				reply(w, 400, map[string]string{"error": "invalid after"})
				return
			}
		}
		if v := r.URL.Query().Get("limit"); v != "" {
			limit, err = strconv.Atoi(v)
			if err != nil || limit < 1 || limit > 1000 {
				reply(w, 400, map[string]string{"error": "limit must be 1..1000"})
				return
			}
		}
		jobs, err := s.List(r.Context(), after, limit)
		if err != nil {
			fail(w, err)
			return
		}
		next := after
		if len(jobs) > 0 {
			next = jobs[len(jobs)-1].ID
		}
		reply(w, 200, map[string]any{"jobs": jobs, "next_after": next})
	})
	return m
}
