package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/task-soup/queue/internal/queue"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	lease, e := time.ParseDuration(env("LEASE_DURATION", "2s"))
	if e != nil {
		return e
	}
	max, e := strconv.Atoi(env("MAX_ATTEMPTS", "10"))
	if e != nil {
		return e
	}
	s, e := queue.Open(ctx, env("DATABASE_URL", "postgres://soup:soup@localhost:54329/soup?sslmode=disable"), lease, max)
	if e != nil {
		return e
	}
	defer s.Pool.Close()
	mode := "api"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	switch mode {
	case "migrate":
		return s.Migrate(ctx)
	case "api":
		srv := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: queue.Handler(s), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
		go func() {
			<-ctx.Done()
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			srv.Shutdown(c)
		}()
		slog.Info("api listening", "addr", srv.Addr)
		e = srv.ListenAndServe()
		if errors.Is(e, http.ErrServerClosed) {
			return nil
		}
		return e
	case "worker":
		slog.Info("worker started", "pid", os.Getpid())
		for ctx.Err() == nil {
			j, err := s.Claim(ctx)
			if err != nil {
				if !errors.Is(err, queue.ErrNotFound) && ctx.Err() == nil {
					slog.Error("claim", "error", err)
				}
				select {
				case <-ctx.Done():
				case <-time.After(20 * time.Millisecond):
				}
				continue
			}
			if j.State != "running" {
				continue
			}

			if !execute(ctx, s, j) {
				continue
			}
			ok, err := s.Complete(ctx, j)
			if err != nil {
				slog.Error("complete", "job", j.ID, "error", err)
			} else if !ok {
				slog.Info("completion fenced", "job", j.ID, "token", j.Token)
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
}

// execute renews a live lease while simulated work proceeds. Loss of ownership stops work.
func execute(ctx context.Context, s *queue.Store, j queue.Job) bool {
	timer := time.NewTimer(time.Duration(j.DurationMS) * time.Millisecond)
	defer timer.Stop()
	tick := time.NewTicker(s.Lease / 3)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case <-tick.C:
			ok, err := s.Renew(ctx, j)
			if err != nil {
				slog.Error("renew", "job", j.ID, "error", err)
				return false
			}
			if !ok {
				return false
			}
		}
	}
}
func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
