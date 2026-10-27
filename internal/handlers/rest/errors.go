package rest

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
)

type APIError struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

type errorMapping struct {
	err     error
	status  int
	code    string
	message string
}

var (
	ErrRouteNotFound       = errors.New("route not found")
	ErrMethodNotAllowed    = errors.New("method not allowed")
	ErrRequestBodyTooLarge = errors.New("request body too large")
)

var errorMappings = []errorMapping{
	{err: ErrRouteNotFound, status: http.StatusNotFound, code: "not_found", message: "route was not found"},
	{err: ErrMethodNotAllowed, status: http.StatusMethodNotAllowed, code: "method_not_allowed", message: "method is not allowed"},
	{err: ErrRequestBodyTooLarge, status: http.StatusRequestEntityTooLarge, code: "validation_error", message: "request body exceeds the allowed size"},
	{err: domain.ErrTenantNotFound, status: http.StatusNotFound, code: "tenant_not_found", message: "tenant was not found"},
	{err: domain.ErrTaskNotFound, status: http.StatusNotFound, code: "task_not_found", message: "task was not found"},
	{err: domain.ErrTenantAlreadyExists, status: http.StatusConflict, code: "tenant_already_exists", message: "a tenant with this slug already exists"},
	{err: domain.ErrTenantUpdateNotAllowed, status: http.StatusConflict, code: "tenant_update_not_allowed", message: "tenant cannot be changed in its current state"},
	{err: domain.ErrTenantVersionConflict, status: http.StatusConflict, code: "tenant_version_conflict", message: "tenant version does not match"},
	{err: domain.ErrInvalidTenant, status: http.StatusBadRequest, code: "validation_error", message: "tenant input is invalid"},
	{err: domain.ErrInvalidTask, status: http.StatusBadRequest, code: "validation_error", message: "task input is invalid"},
	{err: domain.ErrInvalidID, status: http.StatusBadRequest, code: "validation_error", message: "resource ID is invalid"},
	{err: domain.ErrInvalidPagination, status: http.StatusBadRequest, code: "validation_error", message: "pagination parameters are invalid"},
}

func AbortWithError(c *gin.Context, err error) {
	for _, mapping := range errorMappings {
		if errors.Is(err, mapping.err) {
			c.AbortWithStatusJSON(mapping.status, APIError{
				Error: ErrorBody{
					Code:    mapping.code,
					Message: mapping.message,
					Details: map[string]any{},
				},
			})
			return
		}
	}

	slog.Error("unexpected request failure", "error", err)
	c.AbortWithStatusJSON(http.StatusInternalServerError, APIError{
		Error: ErrorBody{
			Code:    "internal_error",
			Message: "an internal error occurred",
			Details: map[string]any{},
		},
	})
}

func ParseLimit(raw string) (int, error) {
	if raw == "" {
		return 50, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.Join(domain.ErrInvalidPagination, err)
	}
	if value <= 0 {
		return 50, nil
	}
	if value > 200 {
		return 200, nil
	}
	return value, nil
}
