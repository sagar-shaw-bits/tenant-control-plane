package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/config"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/handlers/rest/task"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/handlers/rest/tenant"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/pagination"
)

type tenantServiceFake struct {
	tenant domain.Tenant
	task   domain.Task
	err    error
}

func (f *tenantServiceFake) Create(context.Context, string, string) (domain.Tenant, domain.Task, error) {
	return f.tenant, f.task, f.err
}

func (f *tenantServiceFake) Update(context.Context, string, string, int64) (domain.Tenant, domain.Task, error) {
	return f.tenant, f.task, f.err
}

func (f *tenantServiceFake) Delete(context.Context, string) (domain.Tenant, domain.Task, error) {
	return f.tenant, f.task, f.err
}

func (f *tenantServiceFake) Get(context.Context, string) (domain.Tenant, error) {
	return f.tenant, f.err
}

func (f *tenantServiceFake) List(context.Context, string, int, string) (pagination.Page[domain.Tenant], error) {
	return pagination.Page[domain.Tenant]{Items: []domain.Tenant{f.tenant}}, f.err
}

func (f *tenantServiceFake) ListTasks(context.Context, string, int, string) (pagination.Page[domain.Task], error) {
	return pagination.Page[domain.Task]{Items: []domain.Task{f.task}}, f.err
}

type taskServiceFake struct {
	task domain.Task
	err  error
}

func (f *taskServiceFake) Get(context.Context, string) (domain.Task, error) {
	return f.task, f.err
}

func (f *taskServiceFake) List(context.Context, string, string, int, string) (pagination.Page[domain.Task], error) {
	return pagination.Page[domain.Task]{Items: []domain.Task{f.task}}, f.err
}

type readinessFake struct {
	err error
}

func (f readinessFake) Check(context.Context) error { return f.err }

func TestRouterTenantCreateContractAndTimestamps(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Date(2026, 10, 27, 10, 30, 0, 123456789, time.UTC)
	tenantItem := domain.Tenant{
		ID: "123e4567-e89b-12d3-a456-426614174000", Slug: "tenant-abc", Name: "Tenant",
		Status: domain.TenantProvisioning, Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	taskItem := domain.Task{
		ID: "123e4567-e89b-12d3-a456-426614174001", Type: domain.TaskDeploy,
		TenantID: tenantItem.ID, Status: domain.TaskAccepted, CreatedAt: now, UpdatedAt: now,
	}
	router := newTestRouter(t, &tenantServiceFake{tenant: tenantItem, task: taskItem}, &taskServiceFake{}, readinessFake{})

	request := httptest.NewRequest(http.MethodPost, "/v1/tenants", strings.NewReader(`{"slug":"tenant-abc","name":"Tenant"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("POST /v1/tenants status = %d, body %s", response.Code, response.Body)
	}
	for _, expected := range []string{
		`"tenant"`,
		`"task"`,
		`"created_at":"2026-10-27T10:30:00.123Z"`,
		`"updated_at":"2026-10-27T10:30:00.123Z"`,
	} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Errorf("response body does not contain %s: %s", expected, response.Body)
		}
	}
}

func TestRouterStructuredErrorsAndMethodHandling(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := newTestRouter(t, &tenantServiceFake{err: domain.ErrTenantVersionConflict}, &taskServiceFake{}, readinessFake{})

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{name: "unknown route", method: http.MethodGet, path: "/v1/not-a-route", wantStatus: http.StatusNotFound, wantCode: "not_found"},
		{name: "unsupported method", method: http.MethodPut, path: "/v1/tenants", wantStatus: http.StatusMethodNotAllowed, wantCode: "method_not_allowed"},
		{name: "version conflict", method: http.MethodPost, path: "/v1/tenants", body: `{"slug":"tenant-abc","name":"Tenant"}`, wantStatus: http.StatusConflict, wantCode: "tenant_version_conflict"},
		{name: "invalid json", method: http.MethodPost, path: "/v1/tenants", body: `{"slug":`, wantStatus: http.StatusBadRequest, wantCode: "validation_error"},
		{name: "unknown field", method: http.MethodPost, path: "/v1/tenants", body: `{"slug":"tenant-abc","name":"Tenant","admin":true}`, wantStatus: http.StatusBadRequest, wantCode: "validation_error"},
		{name: "malformed limit", method: http.MethodGet, path: "/v1/tenants?limit=many", wantStatus: http.StatusBadRequest, wantCode: "validation_error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d, body %s; want %d", response.Code, response.Body, tt.wantStatus)
			}
			if !strings.Contains(response.Body.String(), `"code":"`+tt.wantCode+`"`) ||
				!strings.Contains(response.Body.String(), `"details":{}`) {
				t.Fatalf("response does not match error contract: %s", response.Body)
			}
		})
	}
}

func TestRouterBodyLimitAndReadiness(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := config.DefaultConfig()
	cfg.RequestBodyLimit = 12
	router, err := NewRouter(cfg, tenant.NewHandler(&tenantServiceFake{}), task.NewHandler(&taskServiceFake{}), readinessFake{})
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/tenants", strings.NewReader(`{"slug":"tenant-abc","name":"Tenant"}`))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized request status = %d, body %s", response.Code, response.Body)
	}

	readyRouter := newTestRouter(t, &tenantServiceFake{}, &taskServiceFake{}, readinessFake{err: errors.New("db unavailable")})
	response = httptest.NewRecorder()
	readyRouter.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), `"code":"not_ready"`) {
		t.Fatalf("unready response = %d %s", response.Code, response.Body)
	}

	response = httptest.NewRecorder()
	readyRouter.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("health response status = %d, want 200", response.Code)
	}
}

func newTestRouter(t *testing.T, tenantService *tenantServiceFake, taskService *taskServiceFake, ready readinessFake) *gin.Engine {
	t.Helper()
	router, err := NewRouter(config.DefaultConfig(), tenant.NewHandler(tenantService), task.NewHandler(taskService), ready)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	return router
}
