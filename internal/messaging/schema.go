package messaging

import (
	"time"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
)

type TaskEvent struct {
	ID       string            `json:"id"`
	Type     domain.TaskType   `json:"type"`
	TenantID string            `json:"tenant_id"`
	Status   domain.TaskStatus `json:"status"`
}

type WorkerUpdate struct {
	UpdateID   string            `json:"update_id"`
	TaskID     string            `json:"task_id"`
	Status     domain.TaskStatus `json:"status"`
	OccurredAt time.Time         `json:"occurred_at"`
	Error      string            `json:"error,omitempty"`
}
