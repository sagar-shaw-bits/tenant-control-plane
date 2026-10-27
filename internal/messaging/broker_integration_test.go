package messaging

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
)

func TestBrokerPublishesPersistentConfirmedEventsAndDeclaresDeadLetterQueue(t *testing.T) {
	brokerURL := os.Getenv("TEST_RABBITMQ_URL")
	if brokerURL == "" {
		t.Skip("TEST_RABBITMQ_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	broker := NewBroker(brokerURL, nil)
	if err := broker.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	t.Cleanup(func() {
		if err := broker.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	if !broker.Connected() {
		t.Fatal("Connected() = false after successful connection")
	}

	event := TaskEvent{
		ID:       "123e4567-e89b-12d3-a456-426614174001",
		Type:     domain.TaskDeploy,
		TenantID: "123e4567-e89b-12d3-a456-426614174000",
		Status:   domain.TaskAccepted,
	}
	if err := broker.PublishTask(ctx, event); err != nil {
		t.Fatalf("PublishTask() error = %v", err)
	}

	delivery := consumeOne(t, broker, TaskQueue)
	if delivery.DeliveryMode != amqp.Persistent || delivery.MessageId != event.ID {
		t.Fatalf("delivery properties = mode %d message ID %q", delivery.DeliveryMode, delivery.MessageId)
	}
	var received TaskEvent
	if err := json.Unmarshal(delivery.Body, &received); err != nil {
		t.Fatalf("decode task event: %v", err)
	}
	if received != event {
		t.Fatalf("received event = %+v, want %+v", received, event)
	}
	if err := delivery.Ack(false); err != nil {
		t.Fatalf("ack task event: %v", err)
	}

	update := WorkerUpdate{
		UpdateID:   "123e4567-e89b-12d3-a456-426614174002",
		TaskID:     event.ID,
		Status:     domain.TaskInProgress,
		OccurredAt: time.Now().UTC(),
	}
	if err := broker.PublishUpdate(ctx, update); err != nil {
		t.Fatalf("PublishUpdate() error = %v", err)
	}
	updateDelivery := consumeOne(t, broker, UpdateQueue)
	if updateDelivery.DeliveryMode != amqp.Persistent || updateDelivery.MessageId != update.UpdateID {
		t.Fatalf("update delivery properties = mode %d message ID %q", updateDelivery.DeliveryMode, updateDelivery.MessageId)
	}
	if err := updateDelivery.Ack(false); err != nil {
		t.Fatalf("ack worker update: %v", err)
	}

	consumerChannel, _, _, err := broker.OpenUpdateConsumer(10)
	if err != nil {
		t.Fatalf("open update consumer: %v", err)
	}
	if err := consumerChannel.Close(); err != nil {
		t.Fatalf("close update consumer channel: %v", err)
	}
}

func consumeOne(t *testing.T, broker *Broker, queue string) amqp.Delivery {
	t.Helper()
	broker.mu.Lock()
	channel, err := broker.conn.Channel()
	broker.mu.Unlock()
	if err != nil {
		t.Fatalf("open test consumer channel: %v", err)
	}
	t.Cleanup(func() {
		if err := channel.Close(); err != nil {
			t.Errorf("close test consumer channel: %v", err)
		}
	})
	if err := channel.Qos(1, 0, false); err != nil {
		t.Fatalf("set test consumer QoS: %v", err)
	}
	deliveries, err := channel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume queue %s: %v", queue, err)
	}
	select {
	case delivery := <-deliveries:
		return delivery
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for message from %s", queue)
		return amqp.Delivery{}
	}
}
