package tenant

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/pagination"
)

type Store interface {
	CreateTenantWithTask(context.Context, domain.Tenant, domain.Task) error
	UpdateTenant(context.Context, string, string, int64, domain.Task) (domain.Tenant, domain.Task, error)
	DeleteTenant(context.Context, string, domain.Task) (domain.Tenant, domain.Task, error)
	GetTenant(context.Context, string) (domain.Tenant, error)
	ListTenants(context.Context, *domain.TenantStatus, int, *pagination.Cursor) (pagination.Page[domain.Tenant], error)
	ListTasksByTenant(context.Context, string, int, *pagination.Cursor) (pagination.Page[domain.Task], error)
}

type Service struct {
	store Store
	now   func() time.Time
}

func NewService(store Store) *Service {
	return &Service{
		store: store,
		now:   time.Now,
	}
}

func (s *Service) Create(ctx context.Context, slug, name string) (domain.Tenant, domain.Task, error) {
	if err := domain.ValidateSlug(slug); err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	if err := domain.ValidateName(name); err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}

	tenantID, err := domain.NewID()
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	taskID, err := domain.NewID()
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}

	now := s.now().UTC()
	tenant := domain.Tenant{
		ID:        tenantID,
		Slug:      slug,
		Name:      strings.TrimSpace(name),
		Status:    domain.TenantProvisioning,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	task := domain.Task{
		ID:        taskID,
		Type:      domain.TaskDeploy,
		TenantID:  tenantID,
		Status:    domain.TaskAccepted,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.store.CreateTenantWithTask(ctx, tenant, task); err != nil {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("create tenant: %w", err)
	}
	return tenant, task, nil
}

func (s *Service) Update(ctx context.Context, id, name string, version int64) (domain.Tenant, domain.Task, error) {
	if err := domain.ValidateID(id); err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	if err := domain.ValidateName(name); err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	if version < 1 {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("%w: version must be positive", domain.ErrInvalidTenant)
	}

	taskID, err := domain.NewID()
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	now := s.now().UTC()
	task := domain.Task{
		ID:        taskID,
		Type:      domain.TaskUpdate,
		TenantID:  id,
		Status:    domain.TaskAccepted,
		CreatedAt: now,
		UpdatedAt: now,
	}

	tenant, task, err := s.store.UpdateTenant(ctx, id, strings.TrimSpace(name), version, task)
	if err != nil {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("update tenant: %w", err)
	}
	return tenant, task, nil
}

func (s *Service) Delete(ctx context.Context, id string) (domain.Tenant, domain.Task, error) {
	if err := domain.ValidateID(id); err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	taskID, err := domain.NewID()
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	now := s.now().UTC()
	task := domain.Task{
		ID:        taskID,
		Type:      domain.TaskDestroy,
		TenantID:  id,
		Status:    domain.TaskAccepted,
		CreatedAt: now,
		UpdatedAt: now,
	}

	tenant, task, err := s.store.DeleteTenant(ctx, id, task)
	if err != nil {
		return domain.Tenant{}, domain.Task{}, fmt.Errorf("delete tenant: %w", err)
	}
	return tenant, task, nil
}

func (s *Service) Get(ctx context.Context, id string) (domain.Tenant, error) {
	if err := domain.ValidateID(id); err != nil {
		return domain.Tenant{}, err
	}
	tenant, err := s.store.GetTenant(ctx, id)
	if err != nil {
		return domain.Tenant{}, fmt.Errorf("get tenant: %w", err)
	}
	return tenant, nil
}

func (s *Service) List(ctx context.Context, status string, limit int, rawCursor string) (pagination.Page[domain.Tenant], error) {
	var tenantStatus *domain.TenantStatus
	if status != "" {
		value := domain.TenantStatus(status)
		switch value {
		case domain.TenantProvisioning, domain.TenantActive, domain.TenantUpdating, domain.TenantDestroying, domain.TenantDestroyed, domain.TenantFailed:
			tenantStatus = &value
		default:
			return pagination.Page[domain.Tenant]{}, fmt.Errorf("%w: unknown tenant status %q", domain.ErrInvalidTenant, status)
		}
	}
	cursor, err := decodeCursor(rawCursor)
	if err != nil {
		return pagination.Page[domain.Tenant]{}, err
	}
	page, err := s.store.ListTenants(ctx, tenantStatus, pagination.NormalizeLimit(limit), cursor)
	if err != nil {
		return pagination.Page[domain.Tenant]{}, fmt.Errorf("list tenants: %w", err)
	}
	return page, nil
}

func (s *Service) ListTasks(ctx context.Context, id string, limit int, rawCursor string) (pagination.Page[domain.Task], error) {
	if err := domain.ValidateID(id); err != nil {
		return pagination.Page[domain.Task]{}, err
	}
	cursor, err := decodeCursor(rawCursor)
	if err != nil {
		return pagination.Page[domain.Task]{}, err
	}
	page, err := s.store.ListTasksByTenant(ctx, id, pagination.NormalizeLimit(limit), cursor)
	if err != nil {
		return pagination.Page[domain.Task]{}, fmt.Errorf("list tenant tasks: %w", err)
	}
	return page, nil
}

func decodeCursor(raw string) (*pagination.Cursor, error) {
	cursor, err := pagination.DecodeCursor(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: cursor is malformed", domain.ErrInvalidPagination)
	}
	return cursor, nil
}
