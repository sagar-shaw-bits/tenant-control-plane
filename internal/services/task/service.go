package task

import (
	"context"
	"fmt"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/pagination"
)

type Store interface {
	GetTask(context.Context, string) (domain.Task, error)
	ListTasks(context.Context, string, *domain.TaskStatus, int, *pagination.Cursor) (pagination.Page[domain.Task], error)
}

type Service struct {
	store Store
}

func NewService(store Store) *Service {
	return &Service{store: store}
}

func (s *Service) Get(ctx context.Context, id string) (domain.Task, error) {
	if err := domain.ValidateID(id); err != nil {
		return domain.Task{}, err
	}
	task, err := s.store.GetTask(ctx, id)
	if err != nil {
		return domain.Task{}, fmt.Errorf("get task: %w", err)
	}
	return task, nil
}

func (s *Service) List(ctx context.Context, tenantID, status string, limit int, rawCursor string) (pagination.Page[domain.Task], error) {
	if tenantID != "" {
		if err := domain.ValidateID(tenantID); err != nil {
			return pagination.Page[domain.Task]{}, err
		}
	}
	var taskStatus *domain.TaskStatus
	if status != "" {
		value := domain.TaskStatus(status)
		switch value {
		case domain.TaskAccepted, domain.TaskInProgress, domain.TaskDone, domain.TaskFailed:
			taskStatus = &value
		default:
			return pagination.Page[domain.Task]{}, fmt.Errorf("%w: unknown task status %q", domain.ErrInvalidTask, status)
		}
	}
	cursor, err := pagination.DecodeCursor(rawCursor)
	if err != nil {
		return pagination.Page[domain.Task]{}, fmt.Errorf("%w: cursor is malformed", domain.ErrInvalidPagination)
	}
	page, err := s.store.ListTasks(ctx, tenantID, taskStatus, pagination.NormalizeLimit(limit), cursor)
	if err != nil {
		return pagination.Page[domain.Task]{}, fmt.Errorf("list tasks: %w", err)
	}
	return page, nil
}
