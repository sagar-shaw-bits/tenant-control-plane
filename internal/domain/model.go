package domain

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

func NewID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate UUID: %w", err)
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		id[0:4], id[4:6], id[6:8], id[8:10], id[10:16]), nil
}

type TenantStatus string

const (
	TenantProvisioning TenantStatus = "provisioning"
	TenantActive       TenantStatus = "active"
	TenantUpdating     TenantStatus = "updating"
	TenantDestroying   TenantStatus = "destroying"
	TenantDestroyed    TenantStatus = "destroyed"
	TenantFailed       TenantStatus = "failed"
)

type TaskType string

const (
	TaskDeploy  TaskType = "deploy"
	TaskUpdate  TaskType = "update"
	TaskDestroy TaskType = "destroy"
)

type TaskStatus string

const (
	TaskAccepted   TaskStatus = "accepted"
	TaskInProgress TaskStatus = "in_progress"
	TaskDone       TaskStatus = "done"
	TaskFailed     TaskStatus = "failed"
)

type Tenant struct {
	ID        string       `json:"id"`
	Slug      string       `json:"slug"`
	Name      string       `json:"name"`
	Status    TenantStatus `json:"status"`
	Version   int64        `json:"version"`
	CreatedAt time.Time    `json:"created_at"`
	UpdatedAt time.Time    `json:"updated_at"`
}

type Task struct {
	ID        string     `json:"id"`
	Type      TaskType   `json:"type"`
	TenantID  string     `json:"tenant_id"`
	Status    TaskStatus `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

func (t Tenant) MarshalJSON() ([]byte, error) {
	type tenantJSON struct {
		ID        string       `json:"id"`
		Slug      string       `json:"slug"`
		Name      string       `json:"name"`
		Status    TenantStatus `json:"status"`
		Version   int64        `json:"version"`
		CreatedAt string       `json:"created_at"`
		UpdatedAt string       `json:"updated_at"`
	}
	return json.Marshal(tenantJSON{
		ID:        t.ID,
		Slug:      t.Slug,
		Name:      t.Name,
		Status:    t.Status,
		Version:   t.Version,
		CreatedAt: FormatTimestamp(t.CreatedAt),
		UpdatedAt: FormatTimestamp(t.UpdatedAt),
	})
}

func (t Task) MarshalJSON() ([]byte, error) {
	type taskJSON struct {
		ID        string     `json:"id"`
		Type      TaskType   `json:"type"`
		TenantID  string     `json:"tenant_id"`
		Status    TaskStatus `json:"status"`
		CreatedAt string     `json:"created_at"`
		UpdatedAt string     `json:"updated_at"`
	}
	return json.Marshal(taskJSON{
		ID:        t.ID,
		Type:      t.Type,
		TenantID:  t.TenantID,
		Status:    t.Status,
		CreatedAt: FormatTimestamp(t.CreatedAt),
		UpdatedAt: FormatTimestamp(t.UpdatedAt),
	})
}

var tenantSlugPattern = regexp.MustCompile(`^[a-z][-a-z0-9]{1,26}[a-z0-9]$`)
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func ValidateID(id string) error {
	if !uuidPattern.MatchString(id) {
		return fmt.Errorf("%w: expected UUID", ErrInvalidID)
	}
	return nil
}

func ValidateSlug(slug string) error {
	if !tenantSlugPattern.MatchString(slug) {
		return fmt.Errorf("%w: slug must match ^[a-z][-a-z0-9]{1,26}[a-z0-9]$", ErrInvalidTenant)
	}
	return nil
}

func ValidateName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidTenant)
	}
	return nil
}

func (t Tenant) Validate() error {
	if err := ValidateID(t.ID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidTenant, err)
	}
	if err := ValidateSlug(t.Slug); err != nil {
		return err
	}
	if err := ValidateName(t.Name); err != nil {
		return err
	}
	if !isTenantStatus(t.Status) {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidTenant, t.Status)
	}
	if t.Version < 1 {
		return fmt.Errorf("%w: version must be positive", ErrInvalidTenant)
	}
	return nil
}

func (t Task) Validate() error {
	if err := ValidateID(t.ID); err != nil {
		return fmt.Errorf("%w: task ID: %w", ErrInvalidTask, err)
	}
	if err := ValidateID(t.TenantID); err != nil {
		return fmt.Errorf("%w: tenant ID: %w", ErrInvalidTask, err)
	}
	if !isTaskType(t.Type) {
		return fmt.Errorf("%w: unknown type %q", ErrInvalidTask, t.Type)
	}
	if !isTaskStatus(t.Status) {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidTask, t.Status)
	}
	return nil
}

func CanTransitionTenant(from, to TenantStatus) bool {
	switch from {
	case TenantProvisioning:
		return to == TenantActive || to == TenantFailed
	case TenantActive:
		return to == TenantUpdating || to == TenantDestroying
	case TenantUpdating:
		return to == TenantActive || to == TenantFailed
	case TenantDestroying:
		return to == TenantDestroyed || to == TenantFailed
	case TenantFailed:
		return to == TenantDestroying
	default:
		return false
	}
}

func CanTransitionTask(from, to TaskStatus) bool {
	switch from {
	case TaskAccepted:
		return to == TaskInProgress
	case TaskInProgress:
		return to == TaskDone || to == TaskFailed
	default:
		return false
	}
}

func (t *Tenant) Transition(to TenantStatus, at time.Time) error {
	if !CanTransitionTenant(t.Status, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTenantTransition, t.Status, to)
	}
	t.Status = to
	t.Version++
	t.UpdatedAt = at.UTC()
	return nil
}

func (t *Task) Transition(to TaskStatus, at time.Time) error {
	if !CanTransitionTask(t.Status, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTaskTransition, t.Status, to)
	}
	t.Status = to
	t.UpdatedAt = at.UTC()
	return nil
}

func FormatTimestamp(at time.Time) string {
	return at.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z")
}

func isTenantStatus(status TenantStatus) bool {
	switch status {
	case TenantProvisioning, TenantActive, TenantUpdating, TenantDestroying, TenantDestroyed, TenantFailed:
		return true
	default:
		return false
	}
}

func isTaskType(taskType TaskType) bool {
	switch taskType {
	case TaskDeploy, TaskUpdate, TaskDestroy:
		return true
	default:
		return false
	}
}

func isTaskStatus(status TaskStatus) bool {
	switch status {
	case TaskAccepted, TaskInProgress, TaskDone, TaskFailed:
		return true
	default:
		return false
	}
}
