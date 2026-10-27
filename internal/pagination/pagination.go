package pagination

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/sagar-shaw-bits/tenant-control-plane/internal/domain"
)

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

type Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

func NormalizeLimit(limit int) int {
	if limit <= 0 {
		return DefaultLimit
	}
	if limit > MaxLimit {
		return MaxLimit
	}
	return limit
}

func EncodeCursor(cursor Cursor) (string, error) {
	if cursor.CreatedAt.IsZero() || domain.ValidateID(cursor.ID) != nil {
		return "", ErrInvalidCursor
	}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func DecodeCursor(raw string) (*Cursor, error) {
	if raw == "" {
		return nil, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	var cursor Cursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return nil, ErrInvalidCursor
	}
	if cursor.CreatedAt.IsZero() || domain.ValidateID(cursor.ID) != nil {
		return nil, ErrInvalidCursor
	}
	cursor.CreatedAt = cursor.CreatedAt.UTC()
	return &cursor, nil
}

var ErrInvalidCursor = errors.New("invalid cursor")
