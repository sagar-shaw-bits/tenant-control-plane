package database

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestOpenAppliesMigrationsAndIsRepeatable(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for range 2 {
		pool, err := Open(ctx, databaseURL)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}

		var tablesExist bool
		err = pool.QueryRow(ctx, `
			SELECT to_regclass('public.tenants') IS NOT NULL
				AND to_regclass('public.tasks') IS NOT NULL
				AND to_regclass('public.outbox') IS NOT NULL
				AND to_regclass('public.processed_updates') IS NOT NULL
				AND to_regclass('public.pending_worker_results') IS NOT NULL
		`).Scan(&tablesExist)
		if err != nil {
			pool.Close()
			t.Fatalf("query migrated tables: %v", err)
		}
		if !tablesExist {
			pool.Close()
			t.Fatal("Open() did not create all expected tables")
		}

		var openTaskIndexExists bool
		err = pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_indexes
				WHERE schemaname = 'public'
					AND indexname = 'tasks_one_open_per_tenant_idx'
			)
		`).Scan(&openTaskIndexExists)
		if err != nil {
			pool.Close()
			t.Fatalf("query partial unique index: %v", err)
		}
		if !openTaskIndexExists {
			pool.Close()
			t.Fatal("Open() did not create the one-open-task index")
		}
		pool.Close()
	}
}
