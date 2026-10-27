package outbox

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/database"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/messaging"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/store"
)

type fakePublisher struct {
	mu       sync.Mutex
	events   []messaging.TaskEvent
	failAt   int
	failWith error
}

func (p *fakePublisher) PublishTask(_ context.Context, event messaging.TaskEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failAt > 0 && len(p.events)+1 == p.failAt {
		return p.failWith
	}
	p.events = append(p.events, event)
	return nil
}

func TestRelayPublishesAndMarksOutboxRows(t *testing.T) {
	pool := openOutboxTestDB(t)
	ctx := context.Background()
	repository := store.NewPostgres(pool)
	tenantID, err := domain.NewID()
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := domain.NewID()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	tenant := domain.Tenant{
		ID: tenantID, Slug: "relay-" + tenantID[:8], Name: "relay tenant",
		Status: domain.TenantProvisioning, Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	task := domain.Task{
		ID: taskID, Type: domain.TaskDeploy, TenantID: tenantID,
		Status: domain.TaskAccepted, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateTenantWithTask(ctx, tenant, task); err != nil {
		t.Fatalf("CreateTenantWithTask() error = %v", err)
	}

	publisher := &fakePublisher{}
	relay, err := NewRelay(pool, publisher, 10, time.Millisecond, nil)
	if err != nil {
		t.Fatalf("NewRelay() error = %v", err)
	}
	if err := relay.PublishBatch(ctx); err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	if countPublishedEvents(publisher.events, taskID) != 1 {
		t.Fatalf("published events = %+v, want task event %s exactly once", publisher.events, taskID)
	}
	assertOutboxPublished(t, ctx, pool, taskID, true)

	if err := relay.PublishBatch(ctx); err != nil {
		t.Fatalf("second PublishBatch() error = %v", err)
	}
	if countPublishedEvents(publisher.events, taskID) != 1 {
		t.Fatalf("second batch republished task event %s: %+v", taskID, publisher.events)
	}
}

func TestRelayLeavesRowsUnpublishedWhenPublisherFails(t *testing.T) {
	pool := openOutboxTestDB(t)
	ctx := context.Background()
	repository := store.NewPostgres(pool)
	tenantID, err := domain.NewID()
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := domain.NewID()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	tenant := domain.Tenant{
		ID: tenantID, Slug: "relay-" + tenantID[:8], Name: "relay tenant",
		Status: domain.TenantProvisioning, Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	task := domain.Task{
		ID: taskID, Type: domain.TaskDeploy, TenantID: tenantID,
		Status: domain.TaskAccepted, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateTenantWithTask(ctx, tenant, task); err != nil {
		t.Fatalf("CreateTenantWithTask() error = %v", err)
	}

	publisher := &fakePublisher{failAt: 1, failWith: errors.New("broker unavailable")}
	relay, err := NewRelay(pool, publisher, 10, time.Millisecond, nil)
	if err != nil {
		t.Fatalf("NewRelay() error = %v", err)
	}
	if err := relay.PublishBatch(ctx); err == nil {
		t.Fatal("PublishBatch() error = nil, want publisher error")
	}
	assertOutboxPublished(t, ctx, pool, taskID, false)
}

func openOutboxTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func assertOutboxPublished(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string, want bool) {
	t.Helper()
	var publishedAt *time.Time
	if err := pool.QueryRow(ctx, "SELECT published_at FROM outbox WHERE id = $1", id).Scan(&publishedAt); err != nil {
		t.Fatalf("read outbox publication state: %v", err)
	}
	if (publishedAt != nil) != want {
		t.Fatalf("published_at = %v, want published=%t", publishedAt, want)
	}
}

func countPublishedEvents(events []messaging.TaskEvent, id string) int {
	count := 0
	for _, event := range events {
		if event.ID == id {
			count++
		}
	}
	return count
}
