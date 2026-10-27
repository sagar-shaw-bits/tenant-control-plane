package task

import (
	"context"
	"errors"
	"testing"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/pagination"
)

type fakeStore struct {
	task domain.Task
	err  error
	id   string

	page     pagination.Page[domain.Task]
	listErr  error
	tenantID string
	status   *domain.TaskStatus
	limit    int
	cursor   *pagination.Cursor
}

func (f *fakeStore) GetTask(_ context.Context, id string) (domain.Task, error) {
	f.id = id
	return f.task, f.err
}

func (f *fakeStore) ListTasks(_ context.Context, tenantID string, status *domain.TaskStatus, limit int, cursor *pagination.Cursor) (pagination.Page[domain.Task], error) {
	f.tenantID, f.status, f.limit, f.cursor = tenantID, status, limit, cursor
	return f.page, f.listErr
}

func TestGetTask(t *testing.T) {
	expected := domain.Task{ID: "123e4567-e89b-12d3-a456-426614174001", Status: domain.TaskAccepted}
	store := &fakeStore{task: expected}
	got, err := NewService(store).Get(context.Background(), expected.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != expected || store.id != expected.ID {
		t.Fatalf("Get() = %+v, store id = %q", got, store.id)
	}
}

func TestGetTaskRejectsEmptyID(t *testing.T) {
	store := &fakeStore{}
	_, err := NewService(store).Get(context.Background(), "")
	if !errors.Is(err, domain.ErrInvalidID) {
		t.Fatalf("Get() error = %v, want ErrInvalidID", err)
	}
	if store.id != "" {
		t.Fatal("Get(empty ID) called store")
	}
}

func TestListTasksAppliesFiltersAndMaximumPageSize(t *testing.T) {
	store := &fakeStore{}
	tenantID := "123e4567-e89b-12d3-a456-426614174000"
	_, err := NewService(store).List(context.Background(), tenantID, string(domain.TaskDone), 250, "")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if store.tenantID != tenantID || store.status == nil || *store.status != domain.TaskDone {
		t.Fatalf("List() filters = tenant %q status %v", store.tenantID, store.status)
	}
	if store.limit != pagination.MaxLimit {
		t.Fatalf("List() limit = %d, want %d", store.limit, pagination.MaxLimit)
	}
}

func TestListTasksRejectsInvalidStatus(t *testing.T) {
	store := &fakeStore{}
	_, err := NewService(store).List(context.Background(), "", "unknown", 50, "")
	if !errors.Is(err, domain.ErrInvalidTask) {
		t.Fatalf("List() error = %v, want ErrInvalidTask", err)
	}
	if store.tenantID != "" {
		t.Fatal("List(invalid status) called store")
	}
}
