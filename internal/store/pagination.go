package store

import (
	"context"
	"fmt"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/pagination"
)

func (s *Postgres) ListTenants(ctx context.Context, status *domain.TenantStatus, limit int, cursor *pagination.Cursor) (pagination.Page[domain.Tenant], error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants
		WHERE ($1::text IS NULL OR status = $1)
		  AND ($2::timestamptz IS NULL OR (created_at, id) < ($2, $3::uuid))
		ORDER BY created_at DESC, id DESC
		LIMIT $4
	`, optionalTenantStatus(status), cursorTime(cursor), cursorID(cursor), limit+1)
	if err != nil {
		return pagination.Page[domain.Tenant]{}, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()

	items := make([]domain.Tenant, 0, limit+1)
	for rows.Next() {
		tenant, err := scanTenant(rows)
		if err != nil {
			return pagination.Page[domain.Tenant]{}, fmt.Errorf("scan tenant: %w", err)
		}
		items = append(items, tenant)
	}
	if err := rows.Err(); err != nil {
		return pagination.Page[domain.Tenant]{}, fmt.Errorf("read tenants: %w", err)
	}
	return tenantPage(items, limit)
}

func (s *Postgres) ListTasks(ctx context.Context, tenantID string, status *domain.TaskStatus, limit int, cursor *pagination.Cursor) (pagination.Page[domain.Task], error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, type, tenant_id, status, created_at, updated_at
		FROM tasks
		WHERE ($1::uuid IS NULL OR tenant_id = $1)
		  AND ($2::text IS NULL OR status = $2)
		  AND ($3::timestamptz IS NULL OR (created_at, id) < ($3, $4::uuid))
		ORDER BY created_at DESC, id DESC
		LIMIT $5
	`, optionalUUID(tenantID), optionalTaskStatus(status), cursorTime(cursor), cursorID(cursor), limit+1)
	if err != nil {
		return pagination.Page[domain.Task]{}, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()

	items := make([]domain.Task, 0, limit+1)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return pagination.Page[domain.Task]{}, fmt.Errorf("scan task: %w", err)
		}
		items = append(items, task)
	}
	if err := rows.Err(); err != nil {
		return pagination.Page[domain.Task]{}, fmt.Errorf("read tasks: %w", err)
	}
	return taskPage(items, limit)
}

func (s *Postgres) ListTasksByTenant(ctx context.Context, tenantID string, limit int, cursor *pagination.Cursor) (pagination.Page[domain.Task], error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, type, tenant_id, status, created_at, updated_at
		FROM tasks
		WHERE tenant_id = $1
		  AND ($2::timestamptz IS NULL OR (created_at, id) < ($2, $3::uuid))
		ORDER BY created_at DESC, id DESC
		LIMIT $4
	`, tenantID, cursorTime(cursor), cursorID(cursor), limit+1)
	if err != nil {
		return pagination.Page[domain.Task]{}, fmt.Errorf("list tenant tasks: %w", err)
	}
	defer rows.Close()

	items := make([]domain.Task, 0, limit+1)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return pagination.Page[domain.Task]{}, fmt.Errorf("scan tenant task: %w", err)
		}
		items = append(items, task)
	}
	if err := rows.Err(); err != nil {
		return pagination.Page[domain.Task]{}, fmt.Errorf("read tenant tasks: %w", err)
	}
	return taskPage(items, limit)
}

func optionalTenantStatus(status *domain.TenantStatus) any {
	if status == nil {
		return nil
	}
	return string(*status)
}

func optionalTaskStatus(status *domain.TaskStatus) any {
	if status == nil {
		return nil
	}
	return string(*status)
}

func optionalUUID(id string) any {
	if id == "" {
		return nil
	}
	return id
}

func cursorTime(cursor *pagination.Cursor) any {
	if cursor == nil {
		return nil
	}
	return cursor.CreatedAt
}

func cursorID(cursor *pagination.Cursor) any {
	if cursor == nil {
		return nil
	}
	return cursor.ID
}

func tenantPage(items []domain.Tenant, limit int) (pagination.Page[domain.Tenant], error) {
	page := pagination.Page[domain.Tenant]{Items: items}
	if len(items) <= limit {
		return page, nil
	}
	page.Items = items[:limit]
	last := page.Items[len(page.Items)-1]
	cursor, err := pagination.EncodeCursor(pagination.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	if err != nil {
		return pagination.Page[domain.Tenant]{}, fmt.Errorf("encode tenant page cursor: %w", err)
	}
	page.NextCursor = cursor
	return page, nil
}

func taskPage(items []domain.Task, limit int) (pagination.Page[domain.Task], error) {
	page := pagination.Page[domain.Task]{Items: items}
	if len(items) <= limit {
		return page, nil
	}
	page.Items = items[:limit]
	last := page.Items[len(page.Items)-1]
	cursor, err := pagination.EncodeCursor(pagination.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	if err != nil {
		return pagination.Page[domain.Task]{}, fmt.Errorf("encode task page cursor: %w", err)
	}
	page.NextCursor = cursor
	return page, nil
}
