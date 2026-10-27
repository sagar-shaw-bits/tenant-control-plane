package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/database"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/messaging"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/pagination"
)

func TestPostgresTransactionalOperations(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	defer pool.Close()

	repository := NewPostgres(pool)
	tenant, deployTask := newTestTenantAndTask(t, domain.TaskDeploy)
	if err := repository.CreateTenantWithTask(ctx, tenant, deployTask); err != nil {
		t.Fatalf("CreateTenantWithTask() error = %v", err)
	}

	assertOutboxEvent(t, ctx, pool, deployTask)

	duplicateTenant, duplicateTask := newTestTenantAndTask(t, domain.TaskDeploy)
	duplicateTenant.Slug = tenant.Slug
	err = repository.CreateTenantWithTask(ctx, duplicateTenant, duplicateTask)
	if !errors.Is(err, domain.ErrTenantAlreadyExists) {
		t.Fatalf("duplicate slug error = %v, want ErrTenantAlreadyExists", err)
	}

	unknownUpdateID, err := domain.NewID()
	if err != nil {
		t.Fatalf("generate unknown update ID: %v", err)
	}
	unknownTaskID, err := domain.NewID()
	if err != nil {
		t.Fatalf("generate unknown task ID: %v", err)
	}
	_, err = repository.ApplyUpdate(ctx, messaging.WorkerUpdate{
		UpdateID: unknownUpdateID,
		TaskID:   unknownTaskID,
		Status:   domain.TaskInProgress,
	})
	if !errors.Is(err, domain.ErrTaskNotFound) {
		t.Fatalf("unknown task update error = %v, want ErrTaskNotFound", err)
	}

	progressUpdate, err := newWorkerUpdate(deployTask.ID, domain.TaskInProgress)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repository.ApplyUpdate(ctx, progressUpdate); err != nil || !applied {
		t.Fatalf("ApplyUpdate(in_progress) = (%t, %v), want (true, nil)", applied, err)
	}
	doneUpdate, err := newWorkerUpdate(deployTask.ID, domain.TaskDone)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repository.ApplyUpdate(ctx, doneUpdate); err != nil || !applied {
		t.Fatalf("ApplyUpdate(done) = (%t, %v), want (true, nil)", applied, err)
	}
	if applied, err := repository.ApplyUpdate(ctx, doneUpdate); err != nil || applied {
		t.Fatalf("duplicate ApplyUpdate(done) = (%t, %v), want (false, nil)", applied, err)
	}
	staleUpdate, err := newWorkerUpdate(deployTask.ID, domain.TaskInProgress)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repository.ApplyUpdate(ctx, staleUpdate); err != nil || applied {
		t.Fatalf("stale ApplyUpdate(in_progress) = (%t, %v), want (false, nil)", applied, err)
	}

	currentTenant, err := repository.GetTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatalf("GetTenant() error = %v", err)
	}
	if currentTenant.Status != domain.TenantActive || currentTenant.Version != 2 {
		t.Fatalf("tenant after deploy completion = status %q version %d; want active version 2", currentTenant.Status, currentTenant.Version)
	}

	const attempts = 20
	start := make(chan struct{})
	type updateResult struct {
		task domain.Task
		err  error
	}
	results := make(chan updateResult, attempts)
	var workers sync.WaitGroup
	for range attempts {
		taskID, err := domain.NewID()
		if err != nil {
			t.Fatalf("generate update task ID: %v", err)
		}
		task := domain.Task{
			ID:       taskID,
			Type:     domain.TaskUpdate,
			TenantID: tenant.ID,
			Status:   domain.TaskAccepted,
		}
		workers.Add(1)
		go func(task domain.Task) {
			defer workers.Done()
			<-start
			_, _, updateErr := repository.UpdateTenant(ctx, tenant.ID, "updated tenant", currentTenant.Version, task)
			results <- updateResult{task: task, err: updateErr}
		}(task)
	}
	close(start)
	workers.Wait()
	close(results)

	successes := 0
	var winningTask domain.Task
	for result := range results {
		switch {
		case result.err == nil:
			successes++
			winningTask = result.task
		case errors.Is(result.err, domain.ErrTenantUpdateNotAllowed):
			// The first update moves the tenant out of active, so later requests are rejected.
		default:
			t.Errorf("concurrent UpdateTenant() error = %v, want update-not-allowed", result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent update successes = %d, want exactly 1", successes)
	}

	if _, err := repository.GetTask(ctx, winningTask.ID); err != nil {
		t.Fatalf("GetTask(winning update) error = %v", err)
	}
	for _, status := range []domain.TaskStatus{domain.TaskInProgress, domain.TaskDone} {
		update, err := newWorkerUpdate(winningTask.ID, status)
		if err != nil {
			t.Fatal(err)
		}
		if applied, err := repository.ApplyUpdate(ctx, update); err != nil || !applied {
			t.Fatalf("ApplyUpdate(%s) = (%t, %v), want (true, nil)", status, applied, err)
		}
	}

	destroyTaskID, err := domain.NewID()
	if err != nil {
		t.Fatalf("generate destroy task ID: %v", err)
	}
	destroyTask := domain.Task{
		ID:       destroyTaskID,
		Type:     domain.TaskDestroy,
		TenantID: tenant.ID,
		Status:   domain.TaskAccepted,
	}
	deletingTenant, _, err := repository.DeleteTenant(ctx, tenant.ID, destroyTask)
	if err != nil {
		t.Fatalf("DeleteTenant() error = %v", err)
	}
	if deletingTenant.Status != domain.TenantDestroying || deletingTenant.Version != 5 {
		t.Fatalf("tenant after delete request = status %q version %d; want destroying version 5", deletingTenant.Status, deletingTenant.Version)
	}
}

func TestPostgresAppliesTerminalUpdateDeliveredBeforeProgress(t *testing.T) {
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
	defer pool.Close()
	repository := NewPostgres(pool)

	tenant, task := newTestTenantAndTask(t, domain.TaskDeploy)
	if err := repository.CreateTenantWithTask(ctx, tenant, task); err != nil {
		t.Fatalf("CreateTenantWithTask() error = %v", err)
	}

	terminalUpdate, err := newWorkerUpdate(task.ID, domain.TaskDone)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repository.ApplyUpdate(ctx, terminalUpdate); err != nil || !applied {
		t.Fatalf("ApplyUpdate(done before progress) = (%t, %v), want (true, nil)", applied, err)
	}
	if applied, err := repository.ApplyUpdate(ctx, terminalUpdate); err != nil || applied {
		t.Fatalf("duplicate ApplyUpdate(done) = (%t, %v), want (false, nil)", applied, err)
	}

	currentTask, err := repository.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask() before progress = %v", err)
	}
	if currentTask.Status != domain.TaskAccepted {
		t.Fatalf("task status before progress = %q, want accepted", currentTask.Status)
	}
	currentTenant, err := repository.GetTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatalf("GetTenant() before progress = %v", err)
	}
	if currentTenant.Status != domain.TenantProvisioning || currentTenant.Version != 1 {
		t.Fatalf("tenant before progress = status %q version %d; want provisioning version 1", currentTenant.Status, currentTenant.Version)
	}

	progressUpdate, err := newWorkerUpdate(task.ID, domain.TaskInProgress)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repository.ApplyUpdate(ctx, progressUpdate); err != nil || !applied {
		t.Fatalf("ApplyUpdate(in_progress) = (%t, %v), want (true, nil)", applied, err)
	}

	currentTask, err = repository.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask() after progress = %v", err)
	}
	if currentTask.Status != domain.TaskDone {
		t.Fatalf("task status after progress = %q, want done", currentTask.Status)
	}
	currentTenant, err = repository.GetTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatalf("GetTenant() after progress = %v", err)
	}
	if currentTenant.Status != domain.TenantActive || currentTenant.Version != 2 {
		t.Fatalf("tenant after progress = status %q version %d; want active version 2", currentTenant.Status, currentTenant.Version)
	}

	var pendingResults int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM pending_worker_results WHERE task_id = $1", task.ID).Scan(&pendingResults); err != nil {
		t.Fatalf("count deferred worker results: %v", err)
	}
	if pendingResults != 0 {
		t.Fatalf("deferred worker results remaining = %d, want 0", pendingResults)
	}

	failedTaskID, err := domain.NewID()
	if err != nil {
		t.Fatalf("generate update task ID: %v", err)
	}
	updateTask := domain.Task{
		ID:       failedTaskID,
		Type:     domain.TaskUpdate,
		TenantID: tenant.ID,
		Status:   domain.TaskAccepted,
	}
	if _, _, err := repository.UpdateTenant(ctx, tenant.ID, "updated tenant", currentTenant.Version, updateTask); err != nil {
		t.Fatalf("UpdateTenant() for failed result = %v", err)
	}
	failedUpdate, err := newWorkerUpdate(updateTask.ID, domain.TaskFailed)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repository.ApplyUpdate(ctx, failedUpdate); err != nil || !applied {
		t.Fatalf("ApplyUpdate(failed before progress) = (%t, %v), want (true, nil)", applied, err)
	}
	progressUpdate, err = newWorkerUpdate(updateTask.ID, domain.TaskInProgress)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := repository.ApplyUpdate(ctx, progressUpdate); err != nil || !applied {
		t.Fatalf("ApplyUpdate(in_progress after failed) = (%t, %v), want (true, nil)", applied, err)
	}
	currentTask, err = repository.GetTask(ctx, updateTask.ID)
	if err != nil {
		t.Fatalf("GetTask() after failed result = %v", err)
	}
	if currentTask.Status != domain.TaskFailed {
		t.Fatalf("task status after failed result = %q, want failed", currentTask.Status)
	}
	currentTenant, err = repository.GetTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatalf("GetTenant() after failed result = %v", err)
	}
	if currentTenant.Status != domain.TenantFailed || currentTenant.Version != 4 {
		t.Fatalf("tenant after failed result = status %q version %d; want failed version 4", currentTenant.Status, currentTenant.Version)
	}
}

func TestPostgresCreateRollsBackTenantWhenTaskInsertFails(t *testing.T) {
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
	defer pool.Close()
	repository := NewPostgres(pool)

	tenant, task := newTestTenantAndTask(t, domain.TaskDeploy)
	if err := repository.CreateTenantWithTask(ctx, tenant, task); err != nil {
		t.Fatalf("CreateTenantWithTask(first) error = %v", err)
	}

	rolledBackTenant, reusedTask := newTestTenantAndTask(t, domain.TaskDeploy)
	reusedTask.ID = task.ID
	if err := repository.CreateTenantWithTask(ctx, rolledBackTenant, reusedTask); err == nil {
		t.Fatal("CreateTenantWithTask(duplicate task ID) error = nil, want failure")
	}
	if _, err := repository.GetTenant(ctx, rolledBackTenant.ID); !errors.Is(err, domain.ErrTenantNotFound) {
		t.Fatalf("GetTenant(rolled back tenant) error = %v, want ErrTenantNotFound", err)
	}
}

func TestPostgresKeysetPaginationAndFilters(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	defer pool.Close()
	repository := NewPostgres(pool)

	var createdTenants []domain.Tenant
	var createdTasks []domain.Task
	for range 3 {
		tenant, task := newTestTenantAndTask(t, domain.TaskDeploy)
		if err := repository.CreateTenantWithTask(ctx, tenant, task); err != nil {
			t.Fatalf("CreateTenantWithTask() error = %v", err)
		}
		if _, err := pool.Exec(ctx, "UPDATE tenants SET status = 'destroyed' WHERE id = $1", tenant.ID); err != nil {
			t.Fatalf("isolate tenant fixture: %v", err)
		}
		createdTenants = append(createdTenants, tenant)
		createdTasks = append(createdTasks, task)
	}

	destroyed := domain.TenantDestroyed
	firstPage, err := repository.ListTenants(ctx, &destroyed, 2, nil)
	if err != nil {
		t.Fatalf("ListTenants(first page) error = %v", err)
	}
	if len(firstPage.Items) != 2 || firstPage.NextCursor == "" {
		t.Fatalf("first tenant page = %d items, cursor %q; want 2 items and cursor", len(firstPage.Items), firstPage.NextCursor)
	}
	cursor, err := pagination.DecodeCursor(firstPage.NextCursor)
	if err != nil {
		t.Fatalf("decode tenant next cursor: %v", err)
	}
	secondPage, err := repository.ListTenants(ctx, &destroyed, 2, cursor)
	if err != nil {
		t.Fatalf("ListTenants(second page) error = %v", err)
	}
	if len(secondPage.Items) != 1 || secondPage.NextCursor != "" {
		t.Fatalf("second tenant page = %d items, cursor %q; want 1 item and no cursor", len(secondPage.Items), secondPage.NextCursor)
	}
	if firstPage.Items[0].ID == secondPage.Items[0].ID || firstPage.Items[1].ID == secondPage.Items[0].ID {
		t.Fatal("tenant pagination returned a duplicate item")
	}

	status := domain.TaskAccepted
	taskPage, err := repository.ListTasks(ctx, createdTenants[0].ID, &status, 10, nil)
	if err != nil {
		t.Fatalf("ListTasks(filtered) error = %v", err)
	}
	if len(taskPage.Items) != 1 || taskPage.Items[0].ID != createdTasks[0].ID {
		t.Fatalf("filtered tasks = %+v, want the selected tenant's deploy task", taskPage.Items)
	}

	tenantTasks, err := repository.ListTasksByTenant(ctx, createdTenants[1].ID, 10, nil)
	if err != nil {
		t.Fatalf("ListTasksByTenant() error = %v", err)
	}
	if len(tenantTasks.Items) != 1 || tenantTasks.Items[0].TenantID != createdTenants[1].ID {
		t.Fatalf("tenant tasks = %+v, want one task for tenant %s", tenantTasks.Items, createdTenants[1].ID)
	}
}

func newTestTenantAndTask(t *testing.T, taskType domain.TaskType) (domain.Tenant, domain.Task) {
	t.Helper()
	tenantID, err := domain.NewID()
	if err != nil {
		t.Fatalf("generate tenant ID: %v", err)
	}
	taskID, err := domain.NewID()
	if err != nil {
		t.Fatalf("generate task ID: %v", err)
	}

	now := time.Now().UTC()
	slug := "tenant-" + strings.ReplaceAll(tenantID[:8], "-", "")
	tenant := domain.Tenant{
		ID:        tenantID,
		Slug:      slug,
		Name:      "Test tenant",
		Status:    domain.TenantProvisioning,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	task := domain.Task{
		ID:        taskID,
		Type:      taskType,
		TenantID:  tenantID,
		Status:    domain.TaskAccepted,
		CreatedAt: now,
		UpdatedAt: now,
	}
	return tenant, task
}

func newWorkerUpdate(taskID string, status domain.TaskStatus) (messaging.WorkerUpdate, error) {
	updateID, err := domain.NewID()
	if err != nil {
		return messaging.WorkerUpdate{}, fmt.Errorf("generate worker update ID: %w", err)
	}
	return messaging.WorkerUpdate{
		UpdateID:   updateID,
		TaskID:     taskID,
		Status:     status,
		OccurredAt: time.Now().UTC(),
	}, nil
}

func assertOutboxEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, task domain.Task) {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, "SELECT payload FROM outbox WHERE id = $1", task.ID).Scan(&raw); err != nil {
		t.Fatalf("read outbox payload: %v", err)
	}
	var event messaging.TaskEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatalf("decode outbox payload: %v", err)
	}
	if event.ID != task.ID || event.Type != task.Type || event.TenantID != task.TenantID || event.Status != task.Status {
		t.Fatalf("outbox event = %+v, want task fields from %+v", event, task)
	}
}
