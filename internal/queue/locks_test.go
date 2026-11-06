package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func holdLock(t *testing.T, pool *pgxpool.Pool, query string, args ...any) (pgx.Tx, int) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
	var pid int
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return tx, pid
}

func waitForBlockedQuery(t *testing.T, pool *pgxpool.Pool, blocker int) {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := pool.QueryRow(wait, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=current_schema() AND $1=ANY(pg_blocking_pids(pid)))`, blocker).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case <-wait.Done():
			t.Fatal("operation did not wait for the database lock")
		case <-ticker.C:
		}
	}
}
