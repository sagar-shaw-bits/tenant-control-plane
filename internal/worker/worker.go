package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/messaging"
)

type Broker interface {
	Connect(context.Context) error
	OpenTaskConsumer(int) (*amqp.Channel, <-chan amqp.Delivery, <-chan *amqp.Error, error)
	PublishUpdate(context.Context, messaging.WorkerUpdate) error
}

type taskDelivery interface {
	GetBody() []byte
	Ack(bool) error
	Reject(bool) error
}

type amqpTaskDelivery struct {
	amqp.Delivery
}

func (d amqpTaskDelivery) GetBody() []byte { return d.Body }

type Worker struct {
	broker Broker
	config Config
	logger *slog.Logger
	random *rand.Rand
	randMu sync.Mutex
	seen   map[string]struct{}
	wait   func(context.Context, time.Duration) error
}

func New(broker Broker, cfg Config, logger *slog.Logger) (*Worker, error) {
	if broker == nil {
		return nil, errors.New("worker requires a broker")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate worker config: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		broker: broker,
		config: cfg,
		logger: logger,
		random: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano())^0x9e3779b97f4a7c15)),
		seen:   make(map[string]struct{}),
		wait:   waitWorker,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	reconnectDelay := 250 * time.Millisecond
	for {
		if err := w.broker.Connect(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger.ErrorContext(ctx, "connect worker to RabbitMQ", "error", err, "retry_in", reconnectDelay)
			if err := w.wait(ctx, reconnectDelay); err != nil {
				return nil
			}
			reconnectDelay = min(reconnectDelay*2, 5*time.Second)
			continue
		}

		channel, deliveries, closed, err := w.broker.OpenTaskConsumer(w.config.Prefetch)
		if err != nil {
			w.logger.ErrorContext(ctx, "open task consumer", "error", err, "retry_in", reconnectDelay)
			if err := w.wait(ctx, reconnectDelay); err != nil {
				return nil
			}
			reconnectDelay = min(reconnectDelay*2, 5*time.Second)
			continue
		}
		reconnectDelay = 250 * time.Millisecond
		err = w.consume(ctx, deliveries, closed)
		if closeErr := channel.Close(); closeErr != nil && ctx.Err() == nil {
			w.logger.WarnContext(ctx, "close task consumer channel", "error", closeErr)
		}
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			w.logger.WarnContext(ctx, "task consumer disconnected", "error", err, "retry_in", reconnectDelay)
		}
		if err := w.wait(ctx, reconnectDelay); err != nil {
			return nil
		}
		reconnectDelay = min(reconnectDelay*2, 5*time.Second)
	}
}

func (w *Worker) consume(ctx context.Context, deliveries <-chan amqp.Delivery, closed <-chan *amqp.Error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case closeErr, ok := <-closed:
			if !ok || closeErr == nil {
				return errors.New("RabbitMQ task consumer channel closed")
			}
			return fmt.Errorf("RabbitMQ task consumer channel closed: %w", closeErr)
		case message, ok := <-deliveries:
			if !ok {
				return errors.New("RabbitMQ task delivery channel closed")
			}
			if err := w.Process(ctx, amqpTaskDelivery{Delivery: message}); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("process task delivery: %w", err)
			}
		}
	}
}

func (w *Worker) Process(ctx context.Context, message taskDelivery) error {
	var event messaging.TaskEvent
	if err := json.Unmarshal(message.GetBody(), &event); err != nil {
		w.logger.WarnContext(ctx, "reject malformed task event", "error", err)
		return rejectTask(message, false, "reject malformed task event")
	}
	if err := validateTaskEvent(event); err != nil {
		w.logger.WarnContext(ctx, "reject invalid task event", "task_id", event.ID, "error", err)
		return rejectTask(message, false, "reject invalid task event")
	}
	if event.Status != domain.TaskAccepted {
		w.logger.WarnContext(ctx, "reject task event with unexpected status", "task_id", event.ID, "status", event.Status)
		return rejectTask(message, false, "reject task event with unexpected status")
	}
	if _, ok := w.seen[event.ID]; ok {
		w.logger.InfoContext(ctx, "ack duplicate task delivery", "task_id", event.ID)
		return ackTask(message)
	}

	if err := w.processEvent(ctx, event); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if waitErr := w.wait(ctx, time.Second); waitErr != nil {
			return waitErr
		}
		if rejectErr := rejectTask(message, true, "requeue task after worker failure"); rejectErr != nil {
			return errors.Join(err, rejectErr)
		}
		return err
	}
	w.seen[event.ID] = struct{}{}
	return ackTask(message)
}

func (w *Worker) processEvent(ctx context.Context, event messaging.TaskEvent) error {
	terminalStatus := domain.TaskDone
	if w.randomFloat() < w.config.FailRate {
		terminalStatus = domain.TaskFailed
	}

	if w.config.OutOfOrder {
		if err := w.publishUpdate(ctx, event.ID, terminalStatus); err != nil {
			return err
		}
		if err := w.delay(ctx); err != nil {
			return err
		}
		return w.publishUpdate(ctx, event.ID, domain.TaskInProgress)
	}

	if err := w.publishUpdate(ctx, event.ID, domain.TaskInProgress); err != nil {
		return err
	}
	if err := w.delay(ctx); err != nil {
		return err
	}
	return w.publishUpdate(ctx, event.ID, terminalStatus)
}

func (w *Worker) publishUpdate(ctx context.Context, taskID string, status domain.TaskStatus) error {
	updateID, err := domain.NewID()
	if err != nil {
		return err
	}
	update := messaging.WorkerUpdate{
		UpdateID:   updateID,
		TaskID:     taskID,
		Status:     status,
		OccurredAt: time.Now().UTC(),
	}
	if err := w.broker.PublishUpdate(ctx, update); err != nil {
		return fmt.Errorf("publish %s update for task %s: %w", status, taskID, err)
	}
	if w.randomFloat() < w.config.DuplicateRate {
		if err := w.broker.PublishUpdate(ctx, update); err != nil {
			return fmt.Errorf("publish duplicate %s update for task %s: %w", status, taskID, err)
		}
	}
	return nil
}

func (w *Worker) delay(ctx context.Context) error {
	delayMS := w.config.MinDelayMS
	if extra := w.config.MaxDelayMS - w.config.MinDelayMS; extra > 0 {
		delayMS += w.randomInt64(extra + 1)
	}
	return w.wait(ctx, time.Duration(delayMS)*time.Millisecond)
}

func (w *Worker) randomFloat() float64 {
	w.randMu.Lock()
	defer w.randMu.Unlock()
	return w.random.Float64()
}

func (w *Worker) randomInt64(max int64) int64 {
	w.randMu.Lock()
	defer w.randMu.Unlock()
	return w.random.Int64N(max)
}

func validateTaskEvent(event messaging.TaskEvent) error {
	if err := domain.ValidateID(event.ID); err != nil {
		return fmt.Errorf("task event ID: %w", err)
	}
	if err := domain.ValidateID(event.TenantID); err != nil {
		return fmt.Errorf("tenant ID: %w", err)
	}
	switch event.Type {
	case domain.TaskDeploy, domain.TaskUpdate, domain.TaskDestroy:
		return nil
	default:
		return fmt.Errorf("invalid task type %q", event.Type)
	}
}

func ackTask(message taskDelivery) error {
	if err := message.Ack(false); err != nil {
		return fmt.Errorf("ack task delivery: %w", err)
	}
	return nil
}

func rejectTask(message taskDelivery, requeue bool, operation string) error {
	if err := message.Reject(requeue); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}

func waitWorker(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
