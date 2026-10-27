package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/config"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/messaging"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/worker"
)

func run() error {
	serviceConfig, err := config.LoadFromEnv()
	if err != nil {
		return fmt.Errorf("load service configuration: %w", err)
	}
	workerConfig, err := worker.LoadConfig()
	if err != nil {
		return fmt.Errorf("load worker configuration: %w", err)
	}

	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	workerConfig.RegisterFlags(flags)
	flags.StringVar(&serviceConfig.RabbitMQURL, "rabbitmq-url", serviceConfig.RabbitMQURL, "RabbitMQ connection URL")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return fmt.Errorf("parse worker flags: %w", err)
	}
	if err := workerConfig.Validate(); err != nil {
		return fmt.Errorf("validate worker configuration: %w", err)
	}
	if err := serviceConfig.Validate(); err != nil {
		return fmt.Errorf("validate RabbitMQ configuration: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	broker := messaging.NewBroker(serviceConfig.RabbitMQURL, logger)
	defer func() {
		if err := broker.Close(); err != nil {
			logger.Error("close RabbitMQ broker", "error", err)
		}
	}()

	simulator, err := worker.New(broker, workerConfig, logger)
	if err != nil {
		return fmt.Errorf("create worker simulator: %w", err)
	}
	logger.Info("worker simulator starting",
		"fail_rate", workerConfig.FailRate,
		"min_delay_ms", workerConfig.MinDelayMS,
		"max_delay_ms", workerConfig.MaxDelayMS,
		"duplicate_rate", workerConfig.DuplicateRate,
		"out_of_order", workerConfig.OutOfOrder,
	)
	if err := simulator.Run(ctx); err != nil {
		return fmt.Errorf("run worker simulator: %w", err)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited", "error", err)
		os.Exit(1)
	}
}
