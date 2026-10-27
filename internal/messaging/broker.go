package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	TaskExchange       = "tenant.tasks"
	TaskQueue          = "tenant.tasks.queue"
	TaskRoutingKey     = "tenant.task"
	UpdateExchange     = "tenant.updates"
	UpdateQueue        = "tenant.updates.queue"
	UpdateRoutingKey   = "tenant.update"
	DeadLetterExchange = "tenant.dead"
	DeadLetterQueue    = "tenant.dead.queue"
	DeadLetterKey      = "tenant.dead"
)

type Broker struct {
	url string

	mu       sync.Mutex
	conn     *amqp.Connection
	channel  *amqp.Channel
	confirms <-chan amqp.Confirmation
	returns  <-chan amqp.Return
	closed   <-chan *amqp.Error
	logger   *slog.Logger
}

func NewBroker(url string, logger *slog.Logger) *Broker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Broker{url: url, logger: logger}
}

func (b *Broker) Connect(ctx context.Context) error {
	delay := 250 * time.Millisecond
	for {
		b.mu.Lock()
		connected := b.isConnectedLocked()
		b.mu.Unlock()
		if connected {
			return nil
		}

		if err := b.connectOnce(); err == nil {
			return nil
		} else {
			b.logger.WarnContext(ctx, "connect to RabbitMQ", "error", err, "retry_in", delay)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("connect to RabbitMQ: %w", ctx.Err())
		case <-timer.C:
		}
		if delay < 5*time.Second {
			delay *= 2
			if delay > 5*time.Second {
				delay = 5 * time.Second
			}
		}
	}
}

func (b *Broker) Connected() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.isConnectedLocked()
}

func (b *Broker) OpenUpdateConsumer(prefetch int) (*amqp.Channel, <-chan amqp.Delivery, <-chan *amqp.Error, error) {
	return b.openConsumer(UpdateQueue, prefetch)
}

func (b *Broker) OpenTaskConsumer(prefetch int) (*amqp.Channel, <-chan amqp.Delivery, <-chan *amqp.Error, error) {
	return b.openConsumer(TaskQueue, prefetch)
}

func (b *Broker) openConsumer(queue string, prefetch int) (*amqp.Channel, <-chan amqp.Delivery, <-chan *amqp.Error, error) {
	if prefetch <= 0 {
		return nil, nil, nil, errors.New("consumer prefetch must be positive")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.isConnectedLocked() {
		if err := b.connectOnceLocked(); err != nil {
			return nil, nil, nil, fmt.Errorf("connect broker for %s consumer: %w", queue, err)
		}
	}

	channel, err := b.conn.Channel()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open %s consumer channel: %w", queue, err)
	}
	if err := channel.Qos(prefetch, 0, false); err != nil {
		_ = channel.Close()
		return nil, nil, nil, fmt.Errorf("set %s consumer QoS: %w", queue, err)
	}
	deliveries, err := channel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		_ = channel.Close()
		return nil, nil, nil, fmt.Errorf("consume %s: %w", queue, err)
	}
	closed := channel.NotifyClose(make(chan *amqp.Error, 1))
	return channel, deliveries, closed, nil
}

func (b *Broker) PublishTask(ctx context.Context, event TaskEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode task event: %w", err)
	}
	return b.publishConfirmed(ctx, TaskExchange, TaskRoutingKey, event.ID, body)
}

func (b *Broker) PublishUpdate(ctx context.Context, update WorkerUpdate) error {
	body, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("encode worker update: %w", err)
	}
	return b.publishConfirmed(ctx, UpdateExchange, UpdateRoutingKey, update.UpdateID, body)
}

func (b *Broker) publishConfirmed(ctx context.Context, exchange, routingKey, messageID string, body []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.isConnectedLocked() {
		if err := b.connectOnceLocked(); err != nil {
			return fmt.Errorf("reconnect to RabbitMQ: %w", err)
		}
	}

	returns := b.returns
	err := b.channel.PublishWithContext(ctx, exchange, routingKey, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		MessageId:    messageID,
		Timestamp:    time.Now().UTC(),
		Body:         body,
	})
	if err != nil {
		return b.closeAfterError(fmt.Errorf("publish to RabbitMQ: %w", err))
	}

	var (
		confirmed bool
		returned  *amqp.Return
	)
	for !confirmed {
		select {
		case confirmation, ok := <-b.confirms:
			if !ok {
				return b.closeAfterError(errors.New("RabbitMQ publisher confirmation channel closed"))
			}
			if !confirmation.Ack {
				return errors.New("RabbitMQ negatively acknowledged publication")
			}
			confirmed = true
		case publicationReturn, ok := <-returns:
			if !ok {
				return b.closeAfterError(errors.New("RabbitMQ returned-message channel closed"))
			}
			returned = &publicationReturn
		case closeErr, ok := <-b.closed:
			if !ok || closeErr == nil {
				return b.closeAfterError(errors.New("RabbitMQ connection closed before publication confirmation"))
			}
			return b.closeAfterError(fmt.Errorf("RabbitMQ connection closed before publication confirmation: %w", closeErr))
		case <-ctx.Done():
			return b.closeAfterError(fmt.Errorf("wait for RabbitMQ publisher confirmation: %w", ctx.Err()))
		}
	}
	if returned == nil {
		select {
		case publicationReturn, ok := <-returns:
			if ok {
				returned = &publicationReturn
			}
		default:
		}
	}
	if returned != nil {
		return fmt.Errorf("RabbitMQ returned unroutable publication: reply code %d, text %q", returned.ReplyCode, returned.ReplyText)
	}
	return nil
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closeLocked()
}

func (b *Broker) connectOnce() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.connectOnceLocked()
}

func (b *Broker) connectOnceLocked() error {
	if err := b.closeLocked(); err != nil {
		return fmt.Errorf("close previous RabbitMQ connection: %w", err)
	}

	conn, err := amqp.Dial(b.url)
	if err != nil {
		return fmt.Errorf("dial RabbitMQ: %w", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	if err := declareTopology(channel); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("declare RabbitMQ topology: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}

	b.conn = conn
	b.channel = channel
	b.confirms = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	b.returns = channel.NotifyReturn(make(chan amqp.Return, 1))
	b.closed = conn.NotifyClose(make(chan *amqp.Error, 1))
	return nil
}

func (b *Broker) isConnectedLocked() bool {
	return b.conn != nil && b.channel != nil && !b.conn.IsClosed() && !b.channel.IsClosed()
}

func (b *Broker) closeLocked() error {
	var errs []error
	if b.channel != nil && !b.channel.IsClosed() {
		if err := b.channel.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close RabbitMQ channel: %w", err))
		}
	}
	if b.conn != nil && !b.conn.IsClosed() {
		if err := b.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close RabbitMQ connection: %w", err))
		}
	}
	b.conn = nil
	b.channel = nil
	b.confirms = nil
	b.returns = nil
	b.closed = nil
	return errors.Join(errs...)
}

func (b *Broker) closeAfterError(err error) error {
	if closeErr := b.closeLocked(); closeErr != nil {
		return errors.Join(err, fmt.Errorf("close RabbitMQ after publication failure: %w", closeErr))
	}
	return err
}

func declareTopology(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(TaskExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare task exchange: %w", err)
	}
	if err := channel.ExchangeDeclare(UpdateExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare update exchange: %w", err)
	}
	if err := channel.ExchangeDeclare(DeadLetterExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead-letter exchange: %w", err)
	}
	taskArgs := amqp.Table{
		"x-dead-letter-exchange":    DeadLetterExchange,
		"x-dead-letter-routing-key": DeadLetterKey,
	}
	if _, err := channel.QueueDeclare(TaskQueue, true, false, false, false, taskArgs); err != nil {
		return fmt.Errorf("declare task queue: %w", err)
	}
	updateArgs := amqp.Table{
		"x-dead-letter-exchange":    DeadLetterExchange,
		"x-dead-letter-routing-key": DeadLetterKey,
	}
	if _, err := channel.QueueDeclare(UpdateQueue, true, false, false, false, updateArgs); err != nil {
		return fmt.Errorf("declare update queue: %w", err)
	}
	if _, err := channel.QueueDeclare(DeadLetterQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead-letter queue: %w", err)
	}
	bindings := []struct {
		queue    string
		key      string
		exchange string
	}{
		{queue: TaskQueue, key: TaskRoutingKey, exchange: TaskExchange},
		{queue: UpdateQueue, key: UpdateRoutingKey, exchange: UpdateExchange},
		{queue: DeadLetterQueue, key: DeadLetterKey, exchange: DeadLetterExchange},
	}
	for _, binding := range bindings {
		if err := channel.QueueBind(binding.queue, binding.key, binding.exchange, false, nil); err != nil {
			return fmt.Errorf("bind %s to %s: %w", binding.queue, binding.exchange, err)
		}
	}
	return nil
}
