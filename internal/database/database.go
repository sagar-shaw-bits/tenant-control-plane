package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sagar-shaw-bits/tenant-control-plane/migrations"
)

func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}

	if err := applyMigrations(databaseURL); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func applyMigrations(databaseURL string) (resultErr error) {
	source, err := iofs.New(migrations.Files, ".")
	if err != nil {
		return fmt.Errorf("create migration source: %w", err)
	}

	runner, err := migrate.NewWithSourceInstance("iofs", source, databaseURL)
	if err != nil {
		if closeErr := source.Close(); closeErr != nil {
			return errors.Join(fmt.Errorf("create migration runner: %w", err), fmt.Errorf("close migration source: %w", closeErr))
		}
		return fmt.Errorf("create migration runner: %w", err)
	}
	defer func() {
		sourceErr, databaseErr := runner.Close()
		if sourceErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close migration source: %w", sourceErr))
		}
		if databaseErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close migration database: %w", databaseErr))
		}
	}()

	if err := runner.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("apply database migrations: %w", err)
	}
	return nil
}
