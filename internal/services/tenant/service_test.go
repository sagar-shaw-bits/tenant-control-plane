package tenant

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/pagination"
)

type fakeStore struct {
	createTenant domain.Tenant
	createTask   domain.Task
	createErr    error

	updateTenant  domain.Tenant
	updateTask    domain.Task
	updateErr     error
	updateID      string
	updateName    string
	updateVersion int64

	deleteTenant domain.Tenant
	deleteTask   domain.Task
	deleteErr    error
	deleteID     string

	getTenant domain.Tenant
	getErr    error
	getID     string

	listTenantsPage pagination.Page[domain.Tenant]
	listTenantsErr  error
	listStatus      *domain.TenantStatus
	listLimit       int
	listCursor      *pagination.Cursor

	listTasksPage pagination.Page[domain.Task]
	listTasksErr  error
	listTasksID   string
}

const testTenantID = "123e4567-e89b-12d3-a456-426614174000"

func (f *fakeStore) CreateTenantWithTask(_ context.Context, tenant domain.Tenant, task domain.Task) error {
	f.createTenant, f.createTask = tenant, task
	return f.createErr
}

func (f *fakeStore) UpdateTenant(_ context.Context, id, name string, version int64, task domain.Task) (domain.Tenant, domain.Task, error) {
	f.updateID, f.updateName, f.updateVersion = id, name, version
	f.updateTask = task
	if f.updateErr != nil {
		return domain.Tenant{}, domain.Task{}, f.updateErr
	}
	return f.updateTenant, task, nil
}

func (f *fakeStore) DeleteTenant(_ context.Context, id string, task domain.Task) (domain.Tenant, domain.Task, error) {
	f.deleteID = id
	if f.deleteErr != nil {
		return domain.Tenant{}, domain.Task{}, f.deleteErr
	}
	f.deleteTask = task
	return f.deleteTenant, task, nil
}

func (f *fakeStore) GetTenant(_ context.Context, id string) (domain.Tenant, error) {
	f.getID = id
	return f.getTenant, f.getErr
}

func (f *fakeStore) ListTenants(_ context.Context, status *domain.TenantStatus, limit int, cursor *pagination.Cursor) (pagination.Page[domain.Tenant], error) {
	f.listStatus, f.listLimit, f.listCursor = status, limit, cursor
	return f.listTenantsPage, f.listTenantsErr
}

func (f *fakeStore) ListTasksByTenant(_ context.Context, id string, limit int, cursor *pagination.Cursor) (pagination.Page[domain.Task], error) {
	f.listTasksID, f.listLimit, f.listCursor = id, limit, cursor
	return f.listTasksPage, f.listTasksErr
}

func TestCreateBuildsProvisioningTenantAndDeployTask(t *testing.T) {
	store := &fakeStore{}
	service := NewService(store)
	now := time.Date(2026, 10, 27, 10, 30, 0, 0, time.FixedZone("local", 5*60*60+30*60))
	service.now = func() time.Time { return now }

	tenant, task, err := service.Create(context.Background(), "tenant-abc", "  Example tenant  ")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if tenant.Status != domain.TenantProvisioning || tenant.Version != 1 || tenant.Name != "Example tenant" {
		t.Fatalf("created tenant has unexpected values: %+v", tenant)
	}
	if task.Type != domain.TaskDeploy || task.Status != domain.TaskAccepted || task.TenantID != tenant.ID {
		t.Fatalf("created task has unexpected values: %+v", task)
	}
	if tenant.ID == "" || task.ID == "" || tenant.ID == task.ID {
		t.Fatalf("generated IDs are invalid: tenant=%q task=%q", tenant.ID, task.ID)
	}
	if !tenant.CreatedAt.Equal(now.UTC()) || !task.CreatedAt.Equal(now.UTC()) {
		t.Fatalf("created timestamps = tenant %s, task %s; want %s", tenant.CreatedAt, task.CreatedAt, now.UTC())
	}
}

func TestCreateValidatesBeforeStorage(t *testing.T) {
	tests := []struct {
		name       string
		slug       string
		tenantName string
	}{
		{name: "invalid slug", slug: "bad-", tenantName: "name"},
		{name: "blank name", slug: "tenant-abc", tenantName: " \t "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{}
			_, _, err := NewService(store).Create(context.Background(), tt.slug, tt.tenantName)
			if !errors.Is(err, domain.ErrInvalidTenant) {
				t.Fatalf("Create() error = %v, want ErrInvalidTenant", err)
			}
			if store.createTenant.ID != "" {
				t.Fatal("Create() called store for invalid input")
			}
		})
	}
}

func TestUpdateCreatesUpdateTaskAndWrapsStoreError(t *testing.T) {
	storeErr := domain.ErrTenantVersionConflict
	store := &fakeStore{updateErr: storeErr}
	_, _, err := NewService(store).Update(context.Background(), testTenantID, "New name", 3)
	if !errors.Is(err, storeErr) {
		t.Fatalf("Update() error = %v, want wrapped version conflict", err)
	}
	if store.updateID != testTenantID || store.updateName != "New name" || store.updateVersion != 3 {
		t.Fatalf("Update() passed unexpected store arguments: %+v", store)
	}
	if store.updateTask.Type != domain.TaskUpdate ||
		store.updateTask.Status != domain.TaskAccepted ||
		store.updateTask.TenantID != testTenantID {
		t.Fatalf("Update() passed unexpected task: %+v", store.updateTask)
	}
}

func TestDeleteCreatesDestroyTask(t *testing.T) {
	store := &fakeStore{}
	_, task, err := NewService(store).Delete(context.Background(), testTenantID)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if task.Type != domain.TaskDestroy || task.Status != domain.TaskAccepted || task.TenantID != testTenantID {
		t.Fatalf("Delete() task = %+v", task)
	}
}

func TestGetRejectsEmptyIDAndWrapsStoreError(t *testing.T) {
	store := &fakeStore{getErr: domain.ErrTenantNotFound}
	service := NewService(store)

	if _, err := service.Get(context.Background(), ""); !errors.Is(err, domain.ErrInvalidID) {
		t.Fatalf("Get(empty ID) error = %v, want ErrInvalidID", err)
	}
	if store.getID != "" {
		t.Fatal("Get(empty ID) called store")
	}
	if _, err := service.Get(context.Background(), testTenantID); !errors.Is(err, domain.ErrTenantNotFound) {
		t.Fatalf("Get() error = %v, want wrapped ErrTenantNotFound", err)
	}
}

func TestListTenantsValidatesFiltersAndPagination(t *testing.T) {
	store := &fakeStore{}
	service := NewService(store)
	rawCursor, err := pagination.EncodeCursor(pagination.Cursor{
		CreatedAt: time.Date(2026, 10, 27, 10, 30, 0, 0, time.UTC),
		ID:        "123e4567-e89b-12d3-a456-426614174000",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.List(context.Background(), "unknown", 1, "")
	if !errors.Is(err, domain.ErrInvalidTenant) {
		t.Fatalf("List(invalid status) error = %v, want ErrInvalidTenant", err)
	}
	if store.listStatus != nil {
		t.Fatal("List(invalid status) called store")
	}

	_, err = service.List(context.Background(), string(domain.TenantActive), 500, rawCursor)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if store.listStatus == nil || *store.listStatus != domain.TenantActive {
		t.Fatalf("List() status = %v, want active", store.listStatus)
	}
	if store.listLimit != pagination.MaxLimit || store.listCursor == nil || store.listCursor.ID != "123e4567-e89b-12d3-a456-426614174000" {
		t.Fatalf("List() pagination = limit %d cursor %+v", store.listLimit, store.listCursor)
	}
}

func TestListTenantTasksRejectsMalformedCursor(t *testing.T) {
	store := &fakeStore{}
	_, err := NewService(store).ListTasks(context.Background(), testTenantID, 20, "invalid")
	if !errors.Is(err, domain.ErrInvalidPagination) {
		t.Fatalf("ListTasks() error = %v, want ErrInvalidPagination", err)
	}
	if store.listTasksID != "" {
		t.Fatal("ListTasks(invalid cursor) called store")
	}
}
