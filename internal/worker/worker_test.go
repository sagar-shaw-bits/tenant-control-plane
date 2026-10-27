package worker

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/messaging"
)

const (
	workerTaskID   = "123e4567-e89b-12d3-a456-426614174001"
	workerTenantID = "123e4567-e89b-12d3-a456-426614174000"
)

type fakeBroker struct {
	updates []messaging.WorkerUpdate
	errAt   int
	err     error
}

func (b *fakeBroker) Connect(context.Context) error { return nil }
func (b *fakeBroker) OpenTaskConsumer(int) (*amqp.Channel, <-chan amqp.Delivery, <-chan *amqp.Error, error) {
	return nil, nil, nil, errors.New("not used")
}
func (b *fakeBroker) PublishUpdate(_ context.Context, update messaging.WorkerUpdate) error {
	if b.errAt > 0 && len(b.updates)+1 == b.errAt {
		return b.err
	}
	b.updates = append(b.updates, update)
	return nil
}

type taskDeliveryFake struct {
	body     []byte
	acked    bool
	rejected bool
	requeue  bool
}

func (d *taskDeliveryFake) GetBody() []byte { return d.body }
func (d *taskDeliveryFake) Ack(multiple bool) error {
	if multiple {
		return errors.New("unexpected multiple ack")
	}
	d.acked = true
	return nil
}
func (d *taskDeliveryFake) Reject(requeue bool) error {
	d.rejected, d.requeue = true, requeue
	return nil
}

func TestProcessTaskPublishesProgressAndTerminalThenAcks(t *testing.T) {
	broker := &fakeBroker{}
	cfg := DefaultConfig()
	cfg.MinDelayMS, cfg.MaxDelayMS = 0, 0
	worker := newTestWorker(t, broker, cfg)
	worker.random = fixedRand(1)
	delivery := taskMessage(t, domain.TaskDeploy)

	if err := worker.Process(context.Background(), delivery); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if !delivery.acked || delivery.rejected || len(broker.updates) != 2 {
		t.Fatalf("ack=%t rejected=%t updates=%+v", delivery.acked, delivery.rejected, broker.updates)
	}
	if broker.updates[0].Status != domain.TaskInProgress || broker.updates[1].Status != domain.TaskDone {
		t.Fatalf("update statuses = %s, %s", broker.updates[0].Status, broker.updates[1].Status)
	}
	if broker.updates[0].UpdateID == broker.updates[1].UpdateID {
		t.Fatal("each update must have a fresh idempotency key")
	}

	duplicate := taskMessage(t, domain.TaskDeploy)
	if err := worker.Process(context.Background(), duplicate); err != nil {
		t.Fatalf("Process(duplicate) error = %v", err)
	}
	if !duplicate.acked || len(broker.updates) != 2 {
		t.Fatalf("duplicate task ack=%t republished updates=%d", duplicate.acked, len(broker.updates))
	}
}

func TestProcessTaskSupportsFailureDuplicateAndOutOfOrderModes(t *testing.T) {
	broker := &fakeBroker{}
	cfg := DefaultConfig()
	cfg.FailRate = 1
	cfg.DuplicateRate = 1
	cfg.OutOfOrder = true
	cfg.MinDelayMS, cfg.MaxDelayMS = 0, 0
	worker := newTestWorker(t, broker, cfg)
	worker.random = fixedRand(0)
	delivery := taskMessage(t, domain.TaskUpdate)

	if err := worker.Process(context.Background(), delivery); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(broker.updates) != 4 {
		t.Fatalf("published updates = %d, want 4 duplicates", len(broker.updates))
	}
	want := []domain.TaskStatus{domain.TaskFailed, domain.TaskFailed, domain.TaskInProgress, domain.TaskInProgress}
	for i, status := range want {
		if broker.updates[i].Status != status {
			t.Errorf("update %d status = %s, want %s", i, broker.updates[i].Status, status)
		}
	}
	if broker.updates[0].UpdateID != broker.updates[1].UpdateID ||
		broker.updates[2].UpdateID != broker.updates[3].UpdateID {
		t.Fatal("duplicate publishes must reuse their original update_id")
	}
}

func TestProcessRejectsPoisonAndRequeuesBrokerFailure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MinDelayMS, cfg.MaxDelayMS = 0, 0
	broker := &fakeBroker{errAt: 1, err: errors.New("broker unavailable")}
	worker := newTestWorker(t, broker, cfg)

	invalid := &taskDeliveryFake{body: []byte(`{"id":"bad"}`)}
	if err := worker.Process(context.Background(), invalid); err != nil {
		t.Fatalf("Process(invalid) error = %v", err)
	}
	if !invalid.rejected || invalid.requeue || invalid.acked {
		t.Fatalf("invalid task delivery state: %+v", invalid)
	}

	redelivery := taskMessage(t, domain.TaskDestroy)
	waitCalls := 0
	worker.wait = func(context.Context, time.Duration) error {
		waitCalls++
		return nil
	}
	if err := worker.Process(context.Background(), redelivery); err == nil {
		t.Fatal("Process(broker failure) error = nil, want publish error")
	}
	if !redelivery.rejected || !redelivery.requeue || redelivery.acked {
		t.Fatalf("failed task delivery state: %+v", redelivery)
	}
	if waitCalls != 1 {
		t.Fatalf("broker failure wait calls = %d, want one backoff", waitCalls)
	}
}

func TestValidateTaskEvent(t *testing.T) {
	event := messaging.TaskEvent{ID: workerTaskID, TenantID: workerTenantID, Type: domain.TaskDeploy, Status: domain.TaskAccepted}
	if err := validateTaskEvent(event); err != nil {
		t.Fatalf("validateTaskEvent() error = %v", err)
	}
	event.Type = "unknown"
	if err := validateTaskEvent(event); err == nil {
		t.Fatal("validateTaskEvent(unknown type) error = nil")
	}
}

func newTestWorker(t *testing.T, broker Broker, cfg Config) *Worker {
	t.Helper()
	worker, err := New(broker, cfg, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	worker.wait = func(context.Context, time.Duration) error { return nil }
	return worker
}

func taskMessage(t *testing.T, taskType domain.TaskType) *taskDeliveryFake {
	t.Helper()
	body, err := json.Marshal(messaging.TaskEvent{
		ID:       workerTaskID,
		Type:     taskType,
		TenantID: workerTenantID,
		Status:   domain.TaskAccepted,
	})
	if err != nil {
		t.Fatalf("marshal task event: %v", err)
	}
	return &taskDeliveryFake{body: body}
}

func fixedRand(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed+1))
}
