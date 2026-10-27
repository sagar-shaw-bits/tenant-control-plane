package pagination

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeLimit(t *testing.T) {
	tests := []struct {
		input int
		want  int
	}{
		{input: 0, want: DefaultLimit},
		{input: -1, want: DefaultLimit},
		{input: 1, want: 1},
		{input: MaxLimit, want: MaxLimit},
		{input: MaxLimit + 1, want: MaxLimit},
	}
	for _, tt := range tests {
		if got := NormalizeLimit(tt.input); got != tt.want {
			t.Errorf("NormalizeLimit(%d) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestCursorRoundTrip(t *testing.T) {
	original := Cursor{
		CreatedAt: time.Date(2026, 10, 27, 10, 30, 0, 123456000, time.FixedZone("local", 5*60*60+30*60)),
		ID:        "123e4567-e89b-12d3-a456-426614174000",
	}
	encoded, err := EncodeCursor(original)
	if err != nil {
		t.Fatalf("EncodeCursor() error = %v", err)
	}
	if strings.Contains(encoded, "+") || strings.Contains(encoded, "/") {
		t.Fatalf("cursor is not URL-safe: %q", encoded)
	}
	decoded, err := DecodeCursor(encoded)
	if err != nil {
		t.Fatalf("DecodeCursor() error = %v", err)
	}
	if !decoded.CreatedAt.Equal(original.CreatedAt) || decoded.ID != original.ID {
		t.Fatalf("decoded cursor = %+v, want %+v", decoded, original)
	}
}

func TestDecodeCursorRejectsMalformedValues(t *testing.T) {
	for _, raw := range []string{"not-base64", "bnVsbA", "e30", "eyJjcmVhdGVkX2F0IjoiMjAyNi0xMC0yN1QxMDozMDowMFoiLCJpZCI6ImJhZCJ9"} {
		if _, err := DecodeCursor(raw); err == nil {
			t.Errorf("DecodeCursor(%q) error = nil, want error", raw)
		}
	}
}
