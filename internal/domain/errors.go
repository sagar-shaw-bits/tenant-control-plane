package domain

import "errors"

var (
	ErrTenantNotFound          = errors.New("tenant not found")
	ErrTenantAlreadyExists     = errors.New("tenant already exists")
	ErrTenantUpdateNotAllowed  = errors.New("tenant update not allowed")
	ErrTenantVersionConflict   = errors.New("tenant version conflict")
	ErrTaskNotFound            = errors.New("task not found")
	ErrInvalidTenant           = errors.New("invalid tenant")
	ErrInvalidTask             = errors.New("invalid task")
	ErrInvalidTenantTransition = errors.New("invalid tenant transition")
	ErrInvalidTaskTransition   = errors.New("invalid task transition")
	ErrInvalidPagination       = errors.New("invalid pagination")
	ErrInvalidID               = errors.New("invalid ID")
)
