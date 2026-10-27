package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/handlers/rest"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/pagination"
)

type Service interface {
	Create(context.Context, string, string) (domain.Tenant, domain.Task, error)
	Update(context.Context, string, string, int64) (domain.Tenant, domain.Task, error)
	Delete(context.Context, string) (domain.Tenant, domain.Task, error)
	Get(context.Context, string) (domain.Tenant, error)
	List(context.Context, string, int, string) (pagination.Page[domain.Tenant], error)
	ListTasks(context.Context, string, int, string) (pagination.Page[domain.Task], error)
}

type Handler struct {
	service Service
}

func NewHandler(service Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) RegisterRoutes(router gin.IRouter) {
	router.POST("/tenants", h.create)
	router.GET("/tenants", h.list)
	router.GET("/tenants/:id", h.get)
	router.PATCH("/tenants/:id", h.update)
	router.DELETE("/tenants/:id", h.delete)
	router.GET("/tenants/:id/tasks", h.listTasks)
}

func (h *Handler) create(c *gin.Context) {
	var request struct {
		Slug string `json:"slug"`
		Name string `json:"name"`
	}
	if err := decodeJSON(c, &request); err != nil {
		rest.AbortWithError(c, err)
		return
	}

	tenant, task, err := h.service.Create(c.Request.Context(), request.Slug, request.Name)
	if err != nil {
		rest.AbortWithError(c, err)
		return
	}
	c.JSON(http.StatusCreated, mutationResponse{Tenant: tenant, Task: task})
}

func (h *Handler) list(c *gin.Context) {
	limit, err := parseLimit(c.Query("limit"))
	if err != nil {
		rest.AbortWithError(c, err)
		return
	}
	page, err := h.service.List(c.Request.Context(), c.Query("status"), limit, c.Query("cursor"))
	if err != nil {
		rest.AbortWithError(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

func (h *Handler) get(c *gin.Context) {
	tenant, err := h.service.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		rest.AbortWithError(c, err)
		return
	}
	c.JSON(http.StatusOK, tenant)
}

func (h *Handler) update(c *gin.Context) {
	var request struct {
		Name    string `json:"name"`
		Version int64  `json:"version"`
	}
	if err := decodeJSON(c, &request); err != nil {
		rest.AbortWithError(c, err)
		return
	}

	tenant, task, err := h.service.Update(c.Request.Context(), c.Param("id"), request.Name, request.Version)
	if err != nil {
		rest.AbortWithError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, mutationResponse{Tenant: tenant, Task: task})
}

func (h *Handler) delete(c *gin.Context) {
	tenant, task, err := h.service.Delete(c.Request.Context(), c.Param("id"))
	if err != nil {
		rest.AbortWithError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, mutationResponse{Tenant: tenant, Task: task})
}

func (h *Handler) listTasks(c *gin.Context) {
	limit, err := parseLimit(c.Query("limit"))
	if err != nil {
		rest.AbortWithError(c, err)
		return
	}
	page, err := h.service.ListTasks(c.Request.Context(), c.Param("id"), limit, c.Query("cursor"))
	if err != nil {
		rest.AbortWithError(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

type mutationResponse struct {
	Tenant domain.Tenant `json:"tenant"`
	Task   domain.Task   `json:"task"`
}

func decodeJSON(c *gin.Context, destination any) error {
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			return rest.ErrRequestBodyTooLarge
		}
		return errors.Join(domain.ErrInvalidTenant, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("request body must contain exactly one JSON value")
		}
		return errors.Join(domain.ErrInvalidTenant, err)
	}
	return nil
}

func parseLimit(raw string) (int, error) {
	return rest.ParseLimit(raw)
}
