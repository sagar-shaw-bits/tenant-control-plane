package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/messaging"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (s *Postgres) CreateTenantWithTask(ctx context.Context, tenant domain.Tenant, task domain.Task) error {
	if err := tenant.Validate(); err != nil {
		return err
	}
	if err := task.Validate(); err != nil {
		return err
	}
	if tenant.Status != domain.TenantProvisioning ||
		task.Type != domain.TaskDeploy ||
		task.Status != domain.TaskAccepted ||
		task.TenantID != tenant.ID {
		return fmt.Errorf("%w: create requires a provisioning tenant and its accepted deploy task", domain.ErrInvalidTask)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tenant creation: %w", err)
	}
	defer rollback(ctx, tx)

	_, err = tx.Exec(ctx, `
		INSERT INTO tenants (id, slug, name, status, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, tenant.ID, tenant.Slug, tenant.Name, tenant.Status, tenant.Version, tenant.CreatedAt, tenant.UpdatedAt)
	if err != nil {
		return mapTenantInsertError(err)
	}
	if err := insertTaskAndOutbox(ctx, tx, task); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tenant creation: %w", err)
	}
	return nil
}

func (s *Postgres) UpdateTenant(ctx context.Context, id, name string, version int64, task domain.Task) (domain.Tenant, domain.Task, error) {
	if err := domain.ValidateName(name); err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	if err := task.Validate(); err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	if task.Type != domain.TaskUpdate || task.Status != domain.TaskAccepted || task.TenantID != id {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("%w: update requires an accepted update task for this tenant", domain.ErrInvalidTask)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("begin tenant update: %w", err)
	}
	defer rollback(ctx, tx)

	tenant, err := scanTenant(tx.QueryRow(ctx, `
		UPDATE tenants
		SET name = $1, status = 'updating', version = version + 1, updated_at = now()
		WHERE id = $2 AND version = $3 AND status = 'active'
		RETURNING id, slug, name, status, version, created_at, updated_at
	`, name, id, version))
	if errors.Is(err, pgx.ErrNoRows) {
		current, readErr := getTenant(ctx, tx, id)
		if errors.Is(readErr, pgx.ErrNoRows) {
			return domain.Tenant{}, domain.Task{}, domain.ErrTenantNotFound
		}
		if readErr != nil {
			return domain.Tenant{}, domain.Task{}, fmt.Errorf("read tenant after update conflict: %w", readErr)
		}
		if current.Status != domain.TenantActive {
			return domain.Tenant{}, domain.Task{}, domain.ErrTenantUpdateNotAllowed
		}
		return domain.Tenant{}, domain.Task{}, domain.ErrTenantVersionConflict
	}
	if err != nil {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("update tenant: %w", err)
	}

	task, err = insertTask(ctx, tx, task)
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	if err := insertOutbox(ctx, tx, task); err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("commit tenant update: %w", err)
	}
	return tenant, task, nil
}

func (s *Postgres) DeleteTenant(ctx context.Context, id string, task domain.Task) (domain.Tenant, domain.Task, error) {
	if err := task.Validate(); err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	if task.Type != domain.TaskDestroy || task.Status != domain.TaskAccepted || task.TenantID != id {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("%w: delete requires an accepted destroy task for this tenant", domain.ErrInvalidTask)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("begin tenant deletion: %w", err)
	}
	defer rollback(ctx, tx)

	tenant, err := scanTenant(tx.QueryRow(ctx, `
		UPDATE tenants
		SET status = 'destroying', version = version + 1, updated_at = now()
		WHERE id = $1 AND status IN ('active', 'failed')
		RETURNING id, slug, name, status, version, created_at, updated_at
	`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		current, readErr := getTenant(ctx, tx, id)
		if errors.Is(readErr, pgx.ErrNoRows) {
			return domain.Tenant{}, domain.Task{}, domain.ErrTenantNotFound
		}
		if readErr != nil {
			return domain.Tenant{}, domain.Task{}, fmt.Errorf("read tenant after delete conflict: %w", readErr)
		}
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("%w: tenant is %s", domain.ErrTenantUpdateNotAllowed, current.Status)
	}
	if err != nil {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("mark tenant for deletion: %w", err)
	}

	task, err = insertTask(ctx, tx, task)
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	if err := insertOutbox(ctx, tx, task); err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("commit tenant deletion: %w", err)
	}
	return tenant, task, nil
}

func (s *Postgres) ApplyUpdate(ctx context.Context, update messaging.WorkerUpdate) (bool, error) {
	if update.UpdateID == "" || update.TaskID == "" {
		return false, fmt.Errorf("%w: update_id and task_id are required", domain.ErrInvalidTask)
	}
	if update.Status != domain.TaskInProgress && update.Status != domain.TaskDone && update.Status != domain.TaskFailed {
		return false, fmt.Errorf("%w: unsupported worker status %q", domain.ErrInvalidTask, update.Status)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin worker update: %w", err)
	}
	defer rollback(ctx, tx)

	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_updates (update_id)
		VALUES ($1)
		ON CONFLICT (update_id) DO NOTHING
	`, update.UpdateID)
	if err != nil {
		return false, fmt.Errorf("record worker update id: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit duplicate worker update: %w", err)
		}
		return false, nil
	}

	var task domain.Task
	err = tx.QueryRow(ctx, `
		SELECT id, type, tenant_id, status, created_at, updated_at
		FROM tasks
		WHERE id = $1
		FOR UPDATE
	`, update.TaskID).Scan(&task.ID, &task.Type, &task.TenantID, &task.Status, &task.CreatedAt, &task.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, domain.ErrTaskNotFound
	}
	if err != nil {
		return false, fmt.Errorf("lock task for worker update: %w", err)
	}

	if task.Status == domain.TaskAccepted && isTerminalTaskStatus(update.Status) {
		_, err := tx.Exec(ctx, `
			INSERT INTO pending_worker_results (task_id, status)
			VALUES ($1, $2)
			ON CONFLICT (task_id) DO NOTHING
		`, task.ID, update.Status)
		if err != nil {
			return false, fmt.Errorf("defer out-of-order terminal result: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit deferred worker result: %w", err)
		}
		return true, nil
	}

	if !domain.CanTransitionTask(task.Status, update.Status) {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit ignored worker update: %w", err)
		}
		return false, nil
	}

	tag, err = tx.Exec(ctx, `
		UPDATE tasks
		SET status = $1, updated_at = now()
		WHERE id = $2
	`, update.Status, task.ID)
	if err != nil {
		return false, fmt.Errorf("apply task status update: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("apply task status update: expected one row, updated %d", tag.RowsAffected())
	}

	if update.Status == domain.TaskInProgress {
		var pendingStatus domain.TaskStatus
		err := tx.QueryRow(ctx, `
			SELECT status
			FROM pending_worker_results
			WHERE task_id = $1
			FOR UPDATE
		`, task.ID).Scan(&pendingStatus)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("read deferred worker result: %w", err)
		}
		if err == nil {
			if err := applyTaskTerminalStatus(ctx, tx, task, pendingStatus); err != nil {
				return false, err
			}
			if _, err := tx.Exec(ctx, `
				DELETE FROM pending_worker_results
				WHERE task_id = $1
			`, task.ID); err != nil {
				return false, fmt.Errorf("delete deferred worker result: %w", err)
			}
		}
	} else if isTerminalTaskStatus(update.Status) {
		if err := applyTenantTerminalStatus(ctx, tx, task, update.Status); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit worker update: %w", err)
	}
	return true, nil
}

func applyTaskTerminalStatus(ctx context.Context, tx pgx.Tx, task domain.Task, status domain.TaskStatus) error {
	tag, err := tx.Exec(ctx, `
		UPDATE tasks
		SET status = $1, updated_at = now()
		WHERE id = $2
	`, status, task.ID)
	if err != nil {
		return fmt.Errorf("apply deferred terminal task status: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("apply deferred terminal task status: expected one row, updated %d", tag.RowsAffected())
	}
	return applyTenantTerminalStatus(ctx, tx, task, status)
}

func applyTenantTerminalStatus(ctx context.Context, tx pgx.Tx, task domain.Task, status domain.TaskStatus) error {
	if isTerminalTaskStatus(status) {
		tenantStatus, err := terminalTenantStatus(task.Type, status)
		if err != nil {
			return err
		}
		expectedTenantStatus := pendingTenantStatus(task.Type)
		tag, err := tx.Exec(ctx, `
			UPDATE tenants
			SET status = $1, version = version + 1, updated_at = now()
			WHERE id = $2 AND status = $3
		`, tenantStatus, task.TenantID, expectedTenantStatus)
		if err != nil {
			return fmt.Errorf("apply tenant terminal status: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("apply terminal task result: tenant %s is not in expected state %s", task.TenantID, expectedTenantStatus)
		}
	}
	return nil
}

func isTerminalTaskStatus(status domain.TaskStatus) bool {
	return status == domain.TaskDone || status == domain.TaskFailed
}

func (s *Postgres) GetTenant(ctx context.Context, id string) (domain.Tenant, error) {
	tenant, err := scanTenant(s.pool.QueryRow(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants
		WHERE id = $1
	`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Tenant{}, domain.ErrTenantNotFound
	}
	if err != nil {
		return domain.Tenant{}, fmt.Errorf("get tenant: %w", err)
	}
	return tenant, nil
}

func (s *Postgres) GetTask(ctx context.Context, id string) (domain.Task, error) {
	task, err := scanTask(s.pool.QueryRow(ctx, `
		SELECT id, type, tenant_id, status, created_at, updated_at
		FROM tasks
		WHERE id = $1
	`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Task{}, domain.ErrTaskNotFound
	}
	if err != nil {
		return domain.Task{}, fmt.Errorf("get task: %w", err)
	}
	return task, nil
}

func insertTaskAndOutbox(ctx context.Context, tx pgx.Tx, task domain.Task) error {
	task, err := insertTask(ctx, tx, task)
	if err != nil {
		return err
	}
	return insertOutbox(ctx, tx, task)
}

func insertTask(ctx context.Context, tx pgx.Tx, task domain.Task) (domain.Task, error) {
	err := tx.QueryRow(ctx, `
		INSERT INTO tasks (id, type, tenant_id, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, type, tenant_id, status, created_at, updated_at
	`, task.ID, task.Type, task.TenantID, task.Status, task.CreatedAt, task.UpdatedAt).Scan(
		&task.ID, &task.Type, &task.TenantID, &task.Status, &task.CreatedAt, &task.UpdatedAt,
	)
	if err != nil {
		return domain.Task{}, fmt.Errorf("insert task: %w", err)
	}
	return task, nil
}

func insertOutbox(ctx context.Context, tx pgx.Tx, task domain.Task) error {
	payload, err := json.Marshal(messaging.TaskEvent{
		ID:       task.ID,
		Type:     task.Type,
		TenantID: task.TenantID,
		Status:   task.Status,
	})
	if err != nil {
		return fmt.Errorf("encode task event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox (id, payload)
		VALUES ($1, $2)
	`, task.ID, payload); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func mapTenantInsertError(err error) error {
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && pgError.Code == "23505" && pgError.ConstraintName == "tenants_slug_key" {
		return errors.Join(domain.ErrTenantAlreadyExists, fmt.Errorf("insert tenant: %w", err))
	}
	return fmt.Errorf("insert tenant: %w", err)
}

func terminalTenantStatus(taskType domain.TaskType, taskStatus domain.TaskStatus) (domain.TenantStatus, error) {
	if taskStatus == domain.TaskFailed {
		return domain.TenantFailed, nil
	}
	if taskStatus != domain.TaskDone {
		return "", fmt.Errorf("%w: %s is not terminal", domain.ErrInvalidTaskTransition, taskStatus)
	}
	switch taskType {
	case domain.TaskDeploy, domain.TaskUpdate:
		return domain.TenantActive, nil
	case domain.TaskDestroy:
		return domain.TenantDestroyed, nil
	default:
		return "", fmt.Errorf("%w: unknown task type %q", domain.ErrInvalidTask, taskType)
	}
}

func pendingTenantStatus(taskType domain.TaskType) domain.TenantStatus {
	switch taskType {
	case domain.TaskDeploy:
		return domain.TenantProvisioning
	case domain.TaskUpdate:
		return domain.TenantUpdating
	case domain.TaskDestroy:
		return domain.TenantDestroying
	default:
		return ""
	}
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTenant(row rowScanner) (domain.Tenant, error) {
	var tenant domain.Tenant
	err := row.Scan(
		&tenant.ID, &tenant.Slug, &tenant.Name, &tenant.Status, &tenant.Version, &tenant.CreatedAt, &tenant.UpdatedAt,
	)
	return tenant, err
}

func scanTask(row rowScanner) (domain.Task, error) {
	var task domain.Task
	err := row.Scan(
		&task.ID, &task.Type, &task.TenantID, &task.Status, &task.CreatedAt, &task.UpdatedAt,
	)
	return task, err
}

func getTenant(ctx context.Context, tx pgx.Tx, id string) (domain.Tenant, error) {
	return scanTenant(tx.QueryRow(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants
		WHERE id = $1
		FOR UPDATE
	`, id))
}

func rollback(ctx context.Context, tx pgx.Tx) {
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		slog.Error("rollback database transaction", "error", err)
	}
}
