package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/config"
	"github.com/sagar-shaw-bits/tenant-control-plane/internal/handlers/rest"
	resttask "github.com/sagar-shaw-bits/tenant-control-plane/internal/handlers/rest/task"
	resttenant "github.com/sagar-shaw-bits/tenant-control-plane/internal/handlers/rest/tenant"
)

type Readiness interface {
	Check(context.Context) error
}

func NewHTTPServer(cfg config.Config, tenantHandler *resttenant.Handler, taskHandler *resttask.Handler, readiness Readiness) (*http.Server, error) {
	router, err := NewRouter(cfg, tenantHandler, taskHandler, readiness)
	if err != nil {
		return nil, err
	}
	return &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}, nil
}

func NewRouter(cfg config.Config, tenantHandler *resttenant.Handler, taskHandler *resttask.Handler, readiness Readiness) (*gin.Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate HTTP router config: %w", err)
	}

	router := gin.New()
	router.HandleMethodNotAllowed = true
	router.Use(requestLogger(), recovery(), bodyLimit(cfg.RequestBodyLimit))

	router.GET("/healthz", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	router.GET("/readyz", func(c *gin.Context) {
		if readiness == nil {
			rest.AbortWithError(c, errors.New("readiness checks are not configured"))
			return
		}
		if err := readiness.Check(c.Request.Context()); err != nil {
			slog.WarnContext(c.Request.Context(), "readiness check failed", "error", err)
			c.JSON(http.StatusServiceUnavailable, rest.APIError{
				Error: rest.ErrorBody{
					Code:    "not_ready",
					Message: "service dependencies are not ready",
					Details: map[string]any{},
				},
			})
			return
		}
		c.Status(http.StatusOK)
	})

	api := router.Group("/v1")
	tenantHandler.RegisterRoutes(api)
	taskHandler.RegisterRoutes(api)
	router.NoRoute(func(c *gin.Context) {
		rest.AbortWithError(c, rest.ErrRouteNotFound)
	})
	router.NoMethod(func(c *gin.Context) {
		rest.AbortWithError(c, rest.ErrMethodNotAllowed)
	})

	return router, nil
}

func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		slog.InfoContext(c.Request.Context(), "HTTP request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration", time.Since(start),
			"client_ip", c.ClientIP(),
		)
	}
}

func recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(c.Request.Context(), "panic handling HTTP request",
					"panic", recovered,
					"stack", string(debug.Stack()),
				)
				if !c.Writer.Written() {
					rest.AbortWithError(c, errors.New("panic handling request"))
				}
				c.Abort()
			}
		}()
		c.Next()
	}
}

func bodyLimit(limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
		c.Next()
	}
}
