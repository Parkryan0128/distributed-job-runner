package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/config"
	"github.com/Parkryan0128/distributed-job-runner/internal/httpapi"
	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("runner stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: runner api|worker|migrate|healthcheck")
	}
	mode := os.Args[1]
	if mode == "healthcheck" {
		address := os.Getenv("LISTEN_ADDR")
		if address == "" {
			address = ":8080"
		}
		return healthcheck(address)
	}
	cfg, err := config.Load(mode)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	pc.MaxConns = int32(max(8, cfg.Concurrency*2+2))
	pc.ConnConfig.ConnectTimeout = 5 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := queue.New(pool)
	op, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := store.Ping(op); err != nil {
		return err
	}
	if mode == "migrate" {
		return store.Migrate(op)
	}
	if mode == "worker" {
		slog.Info("worker started", "worker_id", cfg.WorkerID, "concurrency", cfg.Concurrency, "queues", cfg.Queues)
		w := worker.Worker{Store: store, ID: cfg.WorkerID, Queues: cfg.Queues, Concurrency: cfg.Concurrency, Lease: cfg.Lease, Poll: cfg.Poll}
		return w.Run(ctx)
	}
	demo := httpapi.NewDemo(store)
	demo.SecureCookies = cfg.SecureCookies
	demo.HistoryRetention = cfg.HistoryRetention
	go demo.Run(ctx)
	select {
	case <-demo.Ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	api := httpapi.Server{Store: store, Demo: demo}
	if _, err := os.Stat(cfg.WebDir + "/index.html"); err != nil {
		return fmt.Errorf("dashboard assets: %w", err)
	}
	handler, err := api.Handler(os.DirFS(cfg.WebDir))
	if err != nil {
		return err
	}
	server := http.Server{
		Addr:              cfg.Address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16384,
	}
	done := make(chan error, 1)
	go func() { slog.Info("API started", "address", cfg.Address); done <- server.ListenAndServe() }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}

func healthcheck(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/readyz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("readiness returned %d", resp.StatusCode)
	}
	return nil
}
