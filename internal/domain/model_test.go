package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTenantTransitions(t *testing.T) {
	tests := []struct {
		name string
		from TenantStatus
		to   TenantStatus
		want bool
	}{
		{name: "provisioning to active", from: TenantProvisioning, to: TenantActive, want: true},
		{name: "provisioning to failed", from: TenantProvisioning, to: TenantFailed, want: true},
		{name: "active to updating", from: TenantActive, to: TenantUpdating, want: true},
		{name: "active to destroying", from: TenantActive, to: TenantDestroying, want: true},
		{name: "updating to active", from: TenantUpdating, to: TenantActive, want: true},
		{name: "updating to failed", from: TenantUpdating, to: TenantFailed, want: true},
		{name: "destroying to destroyed", from: TenantDestroying, to: TenantDestroyed, want: true},
		{name: "destroying to failed", from: TenantDestroying, to: TenantFailed, want: true},
		{name: "failed to destroying", from: TenantFailed, to: TenantDestroying, want: true},
		{name: "active cannot become destroyed", from: TenantActive, to: TenantDestroyed},
		{name: "destroyed is terminal", from: TenantDestroyed, to: TenantFailed},
		{name: "failed cannot become active", from: TenantFailed, to: TenantActive},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CanTransitionTenant(tt.from, tt.to); got != tt.want {
				t.Fatalf("CanTransitionTenant(%q, %q) = %t, want %t", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func TestTaskTransitions(t *testing.T) {
	tests := []struct {
		name string
		from TaskStatus
		to   TaskStatus
		want bool
	}{
		{name: "accepted to in progress", from: TaskAccepted, to: TaskInProgress, want: true},
		{name: "in progress to done", from: TaskInProgress, to: TaskDone, want: true},
		{name: "in progress to failed", from: TaskInProgress, to: TaskFailed, want: true},
		{name: "accepted cannot become done", from: TaskAccepted, to: TaskDone},
		{name: "done is terminal", from: TaskDone, to: TaskInProgress},
		{name: "failed is terminal", from: TaskFailed, to: TaskDone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CanTransitionTask(tt.from, tt.to); got != tt.want {
				t.Fatalf("CanTransitionTask(%q, %q) = %t, want %t", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func TestValidateSlug(t *testing.T) {
	tests := []struct {
		slug string
		ok   bool
	}{
		{slug: "abc", ok: true},
		{slug: "a-b2", ok: true},
		{slug: "ab", ok: false},
		{slug: "-ab", ok: false},
		{slug: "ab-", ok: false},
		{slug: "Abc", ok: false},
		{slug: "a_b", ok: false},
		{slug: "a" + strings.Repeat("b", 26) + "c", ok: true},
		{slug: "a" + strings.Repeat("b", 27) + "c", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.slug, func(t *testing.T) {
			err := ValidateSlug(tt.slug)
			if (err == nil) != tt.ok {
				t.Fatalf("ValidateSlug(%q) error = %v, want valid=%t", tt.slug, err, tt.ok)
			}
			if err != nil && !errors.Is(err, ErrInvalidTenant) {
				t.Fatalf("ValidateSlug(%q) error = %v, want ErrInvalidTenant", tt.slug, err)
			}
		})
	}
}

func TestTransitionMutatesVersionAndUTCUpdatedAt(t *testing.T) {
	at := time.Date(2026, 10, 27, 10, 30, 0, 123456789, time.FixedZone("test", 5*60*60+30*60))
	tenant := Tenant{Status: TenantActive, Version: 4}

	if err := tenant.Transition(TenantUpdating, at); err != nil {
		t.Fatalf("Transition() error = %v", err)
	}
	if tenant.Status != TenantUpdating || tenant.Version != 5 {
		t.Fatalf("tenant after transition = status %q, version %d", tenant.Status, tenant.Version)
	}
	if tenant.UpdatedAt.Location() != time.UTC {
		t.Fatalf("UpdatedAt location = %v, want UTC", tenant.UpdatedAt.Location())
	}
}

func TestTransitionRejectsInvalidState(t *testing.T) {
	tenant := Tenant{Status: TenantDestroyed, Version: 2}
	err := tenant.Transition(TenantActive, time.Now())
	if !errors.Is(err, ErrInvalidTenantTransition) {
		t.Fatalf("Transition() error = %v, want ErrInvalidTenantTransition", err)
	}
	if tenant.Status != TenantDestroyed || tenant.Version != 2 {
		t.Fatalf("invalid transition mutated tenant: %+v", tenant)
	}
}

func TestFormatTimestamp(t *testing.T) {
	at := time.Date(2026, 10, 27, 10, 30, 0, 123456789, time.FixedZone("test", 5*60*60+30*60))
	got := FormatTimestamp(at)
	want := "2026-10-27T05:00:00.123Z"
	if got != want {
		t.Fatalf("FormatTimestamp() = %q, want %q", got, want)
	}
}

func TestNewID(t *testing.T) {
	first, err := NewID()
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	if err := ValidateID(first); err != nil {
		t.Fatalf("ValidateID(NewID()) error = %v", err)
	}
	second, err := NewID()
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	if len(first) != 36 || first == second {
		t.Fatalf("NewID() returned invalid or repeated IDs: %q, %q", first, second)
	}
}

func TestValidateID(t *testing.T) {
	tests := []struct {
		id string
		ok bool
	}{
		{id: "123e4567-e89b-12d3-a456-426614174000", ok: true},
		{id: "", ok: false},
		{id: "not-a-uuid", ok: false},
	}
	for _, tt := range tests {
		err := ValidateID(tt.id)
		if (err == nil) != tt.ok {
			t.Errorf("ValidateID(%q) error = %v, want valid=%t", tt.id, err, tt.ok)
		}
	}
}
