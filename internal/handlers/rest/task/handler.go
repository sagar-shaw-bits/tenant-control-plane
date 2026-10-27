package task

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/handlers/rest"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/pagination"
)

type Service interface {
	Get(context.Context, string) (domain.Task, error)
	List(context.Context, string, string, int, string) (pagination.Page[domain.Task], error)
}

type Handler struct {
	service Service
}

func NewHandler(service Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) RegisterRoutes(router gin.IRouter) {
	router.GET("/tasks", h.list)
	router.GET("/tasks/:id", h.get)
}

func (h *Handler) list(c *gin.Context) {
	limit, err := parseLimit(c.Query("limit"))
	if err != nil {
		rest.AbortWithError(c, err)
		return
	}
	page, err := h.service.List(c.Request.Context(), c.Query("tenant_id"), c.Query("status"), limit, c.Query("cursor"))
	if err != nil {
		rest.AbortWithError(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

func (h *Handler) get(c *gin.Context) {
	item, err := h.service.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		rest.AbortWithError(c, err)
		return
	}
	c.JSON(http.StatusOK, item)
}

func parseLimit(raw string) (int, error) {
	limit, err := rest.ParseLimit(raw)
	if err != nil {
		return 0, err
	}
	return limit, nil
}
