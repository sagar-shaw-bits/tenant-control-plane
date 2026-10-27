package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/messaging"
)

type Publisher interface {
	PublishTask(context.Context, messaging.TaskEvent) error
}

type Relay struct {
	pool      *pgxpool.Pool
	publisher Publisher
	batchSize int
	interval  time.Duration
	logger    *slog.Logger
}

func NewRelay(pool *pgxpool.Pool, publisher Publisher, batchSize int, interval time.Duration, logger *slog.Logger) (*Relay, error) {
	if pool == nil {
		return nil, errors.New("outbox relay requires a database pool")
	}
	if publisher == nil {
		return nil, errors.New("outbox relay requires a publisher")
	}
	if batchSize <= 0 {
		return nil, errors.New("outbox batch size must be positive")
	}
	if interval <= 0 {
		return nil, errors.New("outbox poll interval must be positive")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Relay{
		pool:      pool,
		publisher: publisher,
		batchSize: batchSize,
		interval:  interval,
		logger:    logger,
	}, nil
}

func (r *Relay) Run(ctx context.Context) error {
	for {
		if err := r.PublishBatch(ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.ErrorContext(ctx, "outbox relay cycle failed", "error", err, "retry_in", r.interval)
		}

		timer := time.NewTimer(r.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (r *Relay) PublishBatch(ctx context.Context) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin outbox batch: %w", err)
	}
	defer rollback(ctx, tx)

	rows, err := tx.Query(ctx, `
		SELECT id, payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY created_at, id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, r.batchSize)
	if err != nil {
		return fmt.Errorf("select unpublished outbox rows: %w", err)
	}
	type row struct {
		id      string
		payload []byte
	}
	batch := make([]row, 0, r.batchSize)
	for rows.Next() {
		var item row
		if err := rows.Scan(&item.id, &item.payload); err != nil {
			rows.Close()
			return fmt.Errorf("scan outbox row: %w", err)
		}
		batch = append(batch, item)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return fmt.Errorf("read outbox rows: %w", rowsErr)
	}

	for _, item := range batch {
		var event messaging.TaskEvent
		if err := json.Unmarshal(item.payload, &event); err != nil {
			return fmt.Errorf("decode outbox event %s: %w", item.id, err)
		}
		if event.ID != item.id {
			return fmt.Errorf("outbox event ID %s does not match row ID %s", event.ID, item.id)
		}
		if err := r.publisher.PublishTask(ctx, event); err != nil {
			return fmt.Errorf("publish outbox event %s: %w", item.id, err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE outbox
			SET published_at = now()
			WHERE id = $1 AND published_at IS NULL
		`, item.id); err != nil {
			return fmt.Errorf("mark outbox event %s published: %w", item.id, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outbox batch: %w", err)
	}
	return nil
}

func rollback(ctx context.Context, tx pgx.Tx) {
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		slog.ErrorContext(ctx, "rollback outbox transaction", "error", err)
	}
}
