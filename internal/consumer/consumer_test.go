package consumer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/config"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/messaging"
)

const (
	testUpdateID = "123e4567-e89b-12d3-a456-426614174002"
	testTaskID   = "123e4567-e89b-12d3-a456-426614174001"
)

type storeFake struct {
	errs    []error
	applied bool
	calls   int
	update  messaging.WorkerUpdate
}

func (s *storeFake) ApplyUpdate(_ context.Context, update messaging.WorkerUpdate) (bool, error) {
	s.calls++
	s.update = update
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		return false, err
	}
	return s.applied, nil
}

type brokerFake struct {
	connectErr error
}

func (b *brokerFake) Connect(context.Context) error { return b.connectErr }

func (b *brokerFake) OpenUpdateConsumer(int) (*amqp.Channel, <-chan amqp.Delivery, <-chan *amqp.Error, error) {
	return nil, nil, nil, errors.New("not used")
}

type deliveryFake struct {
	body      []byte
	acked     bool
	rejected  bool
	requeue   bool
	ackErr    error
	rejectErr error
}

func (d *deliveryFake) GetBody() []byte { return d.body }
func (d *deliveryFake) Ack(multiple bool) error {
	if multiple {
		return errors.New("multiple ack was not expected")
	}
	d.acked = true
	return d.ackErr
}
func (d *deliveryFake) Reject(requeue bool) error {
	d.rejected = true
	d.requeue = requeue
	return d.rejectErr
}

func TestProcessAcknowledgesAppliedUpdate(t *testing.T) {
	store := &storeFake{applied: true}
	consumer := newTestConsumer(t, store)
	message := &deliveryFake{body: []byte(validUpdateJSON)}

	if err := consumer.Process(context.Background(), message); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if !message.acked || message.rejected || store.calls != 1 {
		t.Fatalf("delivery ack=%t rejected=%t store calls=%d", message.acked, message.rejected, store.calls)
	}
	if store.update.UpdateID != testUpdateID || store.update.TaskID != testTaskID {
		t.Fatalf("applied update = %+v", store.update)
	}
}

func TestProcessRejectsMalformedAndInvalidMessagesImmediately(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "invalid JSON", body: `{"update_id":`},
		{name: "invalid UUID", body: `{"update_id":"bad","task_id":"` + testTaskID + `","status":"done","occurred_at":"2026-10-27T10:00:00Z"}`},
		{name: "unsupported status", body: `{"update_id":"` + testUpdateID + `","task_id":"` + testTaskID + `","status":"accepted","occurred_at":"2026-10-27T10:00:00Z"}`},
		{name: "missing occurred_at", body: `{"update_id":"` + testUpdateID + `","task_id":"` + testTaskID + `","status":"done"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &storeFake{}
			consumer := newTestConsumer(t, store)
			message := &deliveryFake{body: []byte(tt.body)}
			if err := consumer.Process(context.Background(), message); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			if !message.rejected || message.requeue || message.acked {
				t.Fatalf("delivery ack=%t rejected=%t requeue=%t", message.acked, message.rejected, message.requeue)
			}
			if store.calls != 0 {
				t.Fatalf("store called %d times for invalid message", store.calls)
			}
		})
	}
}

func TestProcessRetriesTransientErrorsThenAcknowledges(t *testing.T) {
	store := &storeFake{errs: []error{errors.New("temporary database error"), errors.New("serialization conflict")}, applied: true}
	consumer := newTestConsumer(t, store)
	waitCalls := 0
	consumer.wait = func(context.Context, time.Duration) error {
		waitCalls++
		return nil
	}
	consumer.jitter = func() float64 { return 1 }
	message := &deliveryFake{body: []byte(validUpdateJSON)}

	if err := consumer.Process(context.Background(), message); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if store.calls != 3 || waitCalls != 2 || !message.acked || message.rejected {
		t.Fatalf("calls=%d waits=%d ack=%t rejected=%t", store.calls, waitCalls, message.acked, message.rejected)
	}
}

func TestProcessRejectsUnknownTaskWithoutRetry(t *testing.T) {
	store := &storeFake{errs: []error{domain.ErrTaskNotFound}}
	consumer := newTestConsumer(t, store)
	waitCalls := 0
	consumer.wait = func(context.Context, time.Duration) error {
		waitCalls++
		return nil
	}
	message := &deliveryFake{body: []byte(validUpdateJSON)}

	if err := consumer.Process(context.Background(), message); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if store.calls != 1 || waitCalls != 0 || !message.rejected || message.requeue || message.acked {
		t.Fatalf("calls=%d waits=%d rejected=%t requeue=%t ack=%t", store.calls, waitCalls, message.rejected, message.requeue, message.acked)
	}
}

func TestProcessRejectsAfterConfiguredRetries(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ConsumerMaxRetries = 2
	store := &storeFake{errs: []error{
		errors.New("temporary one"),
		errors.New("temporary two"),
		errors.New("temporary three"),
	}}
	consumer, err := New(store, &brokerFake{}, cfg, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	consumer.wait = func(context.Context, time.Duration) error { return nil }
	consumer.jitter = func() float64 { return 1 }
	message := &deliveryFake{body: []byte(validUpdateJSON)}

	if err := consumer.Process(context.Background(), message); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if store.calls != 3 || !message.rejected || message.requeue || message.acked {
		t.Fatalf("calls=%d rejected=%t requeue=%t ack=%t", store.calls, message.rejected, message.requeue, message.acked)
	}
}

func TestRetryDelayIsExponentialAndBounded(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.ConsumerRetryBaseDelay = 100 * time.Millisecond
	cfg.ConsumerRetryMaxDelay = 250 * time.Millisecond
	consumer, err := New(&storeFake{}, &brokerFake{}, cfg, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	consumer.jitter = func() float64 { return 1 }

	if got := consumer.retryDelay(0); got != 100*time.Millisecond {
		t.Errorf("retryDelay(0) = %s, want 100ms", got)
	}
	if got := consumer.retryDelay(1); got != 200*time.Millisecond {
		t.Errorf("retryDelay(1) = %s, want 200ms", got)
	}
	if got := consumer.retryDelay(2); got != 250*time.Millisecond {
		t.Errorf("retryDelay(2) = %s, want max 250ms", got)
	}
}

func TestProcessReturnsAckFailure(t *testing.T) {
	consumer := newTestConsumer(t, &storeFake{applied: true})
	message := &deliveryFake{body: []byte(validUpdateJSON), ackErr: errors.New("channel closed")}

	err := consumer.Process(context.Background(), message)
	if err == nil || !strings.Contains(err.Error(), "ack worker update") {
		t.Fatalf("Process() error = %v, want explicit ack failure", err)
	}
}

func newTestConsumer(t *testing.T, store Store) *Consumer {
	t.Helper()
	consumer, err := New(store, &brokerFake{}, config.DefaultConfig(), nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	consumer.jitter = func() float64 { return 1 }
	return consumer
}

const validUpdateJSON = `{"update_id":"123e4567-e89b-12d3-a456-426614174002","task_id":"123e4567-e89b-12d3-a456-426614174001","status":"in_progress","occurred_at":"2026-10-27T10:00:00Z"}`
