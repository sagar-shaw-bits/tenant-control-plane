package rest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
)

func TestErrorMappings(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		err    error
		status int
		code   string
	}{
		{err: domain.ErrTenantNotFound, status: http.StatusNotFound, code: "tenant_not_found"},
		{err: domain.ErrTaskNotFound, status: http.StatusNotFound, code: "task_not_found"},
		{err: domain.ErrTenantAlreadyExists, status: http.StatusConflict, code: "tenant_already_exists"},
		{err: domain.ErrTenantUpdateNotAllowed, status: http.StatusConflict, code: "tenant_update_not_allowed"},
		{err: domain.ErrTenantVersionConflict, status: http.StatusConflict, code: "tenant_version_conflict"},
		{err: domain.ErrInvalidTenant, status: http.StatusBadRequest, code: "validation_error"},
		{err: ErrRequestBodyTooLarge, status: http.StatusRequestEntityTooLarge, code: "validation_error"},
		{err: ErrRouteNotFound, status: http.StatusNotFound, code: "not_found"},
		{err: ErrMethodNotAllowed, status: http.StatusMethodNotAllowed, code: "method_not_allowed"},
	}

	for _, tt := range tests {
		response := httptest.NewRecorder()
		context, _ := gin.CreateTestContext(response)
		AbortWithError(context, tt.err)
		if response.Code != tt.status {
			t.Errorf("error %v status = %d, want %d", tt.err, response.Code, tt.status)
		}
		if got := response.Body.String(); !containsErrorCode(got, tt.code) {
			t.Errorf("error %v body %q does not contain code %q", tt.err, got, tt.code)
		}
	}
}

func TestUnknownErrorMapsToSafeInternalError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	response := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(response)
	AbortWithError(context, domain.ErrInvalidTenantTransition)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
	if containsErrorCode(response.Body.String(), "invalid tenant transition") {
		t.Fatalf("response leaked internal error: %s", response.Body)
	}
	if !containsErrorCode(response.Body.String(), "internal_error") {
		t.Fatalf("response lacks internal_error code: %s", response.Body)
	}
}

func containsErrorCode(body, code string) bool {
	return strings.Contains(body, `"code":"`+code+`"`)
}
