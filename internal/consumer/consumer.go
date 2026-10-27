package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/config"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/messaging"
)

type Store interface {
	ApplyUpdate(context.Context, messaging.WorkerUpdate) (bool, error)
}

type Broker interface {
	Connect(context.Context) error
	OpenUpdateConsumer(int) (*amqp.Channel, <-chan amqp.Delivery, <-chan *amqp.Error, error)
}

type delivery interface {
	GetBody() []byte
	Ack(bool) error
	Reject(bool) error
}

type amqpDelivery struct {
	amqp.Delivery
}

func (d amqpDelivery) GetBody() []byte { return d.Body }

type Consumer struct {
	store  Store
	broker Broker
	config config.Config
	logger *slog.Logger
	jitter func() float64
	wait   func(context.Context, time.Duration) error
}

func New(store Store, broker Broker, cfg config.Config, logger *slog.Logger) (*Consumer, error) {
	if store == nil {
		return nil, errors.New("worker update consumer requires a store")
	}
	if broker == nil {
		return nil, errors.New("worker update consumer requires a broker")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate worker update consumer config: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{
		store:  store,
		broker: broker,
		config: cfg,
		logger: logger,
		jitter: func() float64 { return 0.5 + rand.Float64() },
		wait:   waitContext,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	reconnectDelay := 250 * time.Millisecond
	for {
		if err := c.broker.Connect(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.ErrorContext(ctx, "connect worker update consumer", "error", err, "retry_in", reconnectDelay)
			if err := c.wait(ctx, reconnectDelay); err != nil {
				return nil
			}
			reconnectDelay = min(reconnectDelay*2, 5*time.Second)
			continue
		}

		channel, deliveries, closed, err := c.broker.OpenUpdateConsumer(c.config.ConsumerPrefetch)
		if err != nil {
			c.logger.ErrorContext(ctx, "open worker update consumer", "error", err, "retry_in", reconnectDelay)
			if err := c.wait(ctx, reconnectDelay); err != nil {
				return nil
			}
			reconnectDelay = min(reconnectDelay*2, 5*time.Second)
			continue
		}
		reconnectDelay = 250 * time.Millisecond
		err = c.consume(ctx, deliveries, closed)
		if closeErr := channel.Close(); closeErr != nil && ctx.Err() == nil {
			c.logger.WarnContext(ctx, "close worker update consumer channel", "error", closeErr)
		}
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			c.logger.WarnContext(ctx, "worker update consumer disconnected", "error", err, "retry_in", reconnectDelay)
		}
		if err := c.wait(ctx, reconnectDelay); err != nil {
			return nil
		}
		reconnectDelay = min(reconnectDelay*2, 5*time.Second)
	}
}

func (c *Consumer) consume(ctx context.Context, deliveries <-chan amqp.Delivery, closed <-chan *amqp.Error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case closeErr, ok := <-closed:
			if !ok || closeErr == nil {
				return errors.New("RabbitMQ update consumer channel closed")
			}
			return fmt.Errorf("RabbitMQ update consumer channel closed: %w", closeErr)
		case message, ok := <-deliveries:
			if !ok {
				return errors.New("RabbitMQ update delivery channel closed")
			}
			if err := c.Process(ctx, amqpDelivery{Delivery: message}); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("process worker update delivery: %w", err)
			}
			if ctx.Err() != nil {
				return nil
			}
		}
	}
}

func (c *Consumer) Process(ctx context.Context, message delivery) error {
	var update messaging.WorkerUpdate
	if err := json.Unmarshal(message.GetBody(), &update); err != nil {
		c.logger.WarnContext(ctx, "reject malformed worker update", "error", err)
		return reject(message, "reject malformed worker update")
	}
	if err := validateUpdate(update); err != nil {
		c.logger.WarnContext(ctx, "reject invalid worker update", "update_id", update.UpdateID, "task_id", update.TaskID, "error", err)
		return reject(message, "reject invalid worker update")
	}

	for attempt := 0; ; attempt++ {
		applied, err := c.store.ApplyUpdate(ctx, update)
		if err == nil {
			if !applied {
				c.logger.InfoContext(ctx, "worker update ignored as duplicate or stale", "update_id", update.UpdateID, "task_id", update.TaskID, "status", update.Status)
			}
			if ackErr := message.Ack(false); ackErr != nil {
				return fmt.Errorf("ack worker update %s: %w", update.UpdateID, ackErr)
			}
			return nil
		}
		if errors.Is(err, domain.ErrTaskNotFound) || errors.Is(err, domain.ErrInvalidTask) {
			c.logger.WarnContext(ctx, "reject poison worker update", "update_id", update.UpdateID, "task_id", update.TaskID, "error", err)
			return reject(message, "reject poison worker update")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt >= c.config.ConsumerMaxRetries {
			c.logger.ErrorContext(ctx, "worker update retries exhausted", "update_id", update.UpdateID, "task_id", update.TaskID, "attempts", attempt+1, "error", err)
			return reject(message, "reject worker update after retries")
		}

		delay := c.retryDelay(attempt)
		c.logger.WarnContext(ctx, "retry worker update", "update_id", update.UpdateID, "task_id", update.TaskID, "attempt", attempt+1, "retry_in", delay, "error", err)
		if waitErr := c.wait(ctx, delay); waitErr != nil {
			return waitErr
		}
	}
}

func (c *Consumer) retryDelay(retry int) time.Duration {
	delay := c.config.ConsumerRetryBaseDelay
	for range retry {
		if delay >= c.config.ConsumerRetryMaxDelay/2 {
			delay = c.config.ConsumerRetryMaxDelay
			break
		}
		delay *= 2
	}
	delay = min(delay, c.config.ConsumerRetryMaxDelay)
	jittered := time.Duration(float64(delay) * c.jitter())
	return min(jittered, c.config.ConsumerRetryMaxDelay)
}

func validateUpdate(update messaging.WorkerUpdate) error {
	if err := domain.ValidateID(update.UpdateID); err != nil {
		return fmt.Errorf("update_id: %w", err)
	}
	if err := domain.ValidateID(update.TaskID); err != nil {
		return fmt.Errorf("task_id: %w", err)
	}
	if update.Status != domain.TaskInProgress && update.Status != domain.TaskDone && update.Status != domain.TaskFailed {
		return fmt.Errorf("%w: unsupported worker status %q", domain.ErrInvalidTask, update.Status)
	}
	if update.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred_at is required", domain.ErrInvalidTask)
	}
	return nil
}

func reject(message delivery, operation string) error {
	if err := message.Reject(false); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
