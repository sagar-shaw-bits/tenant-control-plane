package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/config"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/consumer"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/database"
	resttask "github.com/sagar-shaw-bits/tenant-control-plane/internal/handlers/rest/task"
	resttenant "github.com/sagar-shaw-bits/tenant-control-plane/internal/handlers/rest/tenant"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/messaging"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/outbox"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/server"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/services/task"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/services/tenant"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/store"
)

func run() (resultErr error) {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	flags := flag.NewFlagSet("control-plane", flag.ContinueOnError)
	cfg.RegisterFlags(flags)
	if err := flags.Parse(os.Args[1:]); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate configuration: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	pool, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer pool.Close()

	broker := messaging.NewBroker(cfg.RabbitMQURL, logger)
	defer func() {
		if err := broker.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close RabbitMQ broker: %w", err))
		}
	}()
	if err := broker.Connect(ctx); err != nil {
		return fmt.Errorf("connect RabbitMQ: %w", err)
	}

	repository := store.NewPostgres(pool)
	tenantService := tenant.NewService(repository)
	taskService := task.NewService(repository)
	tenantHandler := resttenant.NewHandler(tenantService)
	taskHandler := resttask.NewHandler(taskService)

	readiness, err := server.NewDependencyReadiness(pool, broker)
	if err != nil {
		return fmt.Errorf("create readiness checks: %w", err)
	}
	httpServer, err := server.NewHTTPServer(cfg, tenantHandler, taskHandler, readiness)
	if err != nil {
		return fmt.Errorf("create HTTP server: %w", err)
	}

	relay, err := outbox.NewRelay(pool, broker, cfg.OutboxBatchSize, cfg.OutboxPollInterval, logger)
	if err != nil {
		return fmt.Errorf("create outbox relay: %w", err)
	}
	updateConsumer, err := consumer.New(repository, broker, cfg, logger)
	if err != nil {
		return fmt.Errorf("create worker update consumer: %w", err)
	}

	consumerCtx, stopConsumer := context.WithCancel(context.Background())
	relayCtx, stopRelay := context.WithCancel(context.Background())
	consumerDone := make(chan struct{})
	relayDone := make(chan struct{})
	go func() {
		defer close(relayDone)
		if err := relay.Run(relayCtx); err != nil {
			logger.Error("outbox relay stopped", "error", err)
		}
	}()
	go func() {
		defer close(consumerDone)
		if err := updateConsumer.Run(consumerCtx); err != nil {
			logger.Error("worker update consumer stopped", "error", err)
		}
	}()

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		stopConsumer()
		stopRelay()
		if waitErr := waitForShutdown(consumerDone, cfg.ShutdownTimeout); waitErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("stop worker update consumer: %w", waitErr))
		}
		if waitErr := waitForShutdown(relayDone, cfg.ShutdownTimeout); waitErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("stop outbox relay: %w", waitErr))
		}
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("control-plane HTTP server listening", "address", listener.Addr().String())
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
		}
		close(serverErrors)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serverErrors:
		if err != nil {
			resultErr = fmt.Errorf("serve HTTP: %w", err)
		}
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("shut down HTTP server: %w", err))
		if closeErr := httpServer.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("force close HTTP server: %w", closeErr))
		}
	}
	cancelShutdown()

	stopConsumer()
	if err := waitForShutdown(consumerDone, cfg.ShutdownTimeout); err != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("stop worker update consumer: %w", err))
	}
	stopRelay()
	if err := waitForShutdown(relayDone, cfg.ShutdownTimeout); err != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("stop outbox relay: %w", err))
	}

	return resultErr
}

func waitForShutdown(done <-chan struct{}, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return errors.New("shutdown timed out")
	}
}

func main() {
	if err := run(); err != nil {
		slog.Error("control-plane exited", "error", err)
		os.Exit(1)
	}
}
