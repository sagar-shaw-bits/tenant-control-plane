package config

import (
	"flag"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig().Validate() error = %v", err)
	}
	if cfg.OutboxPollInterval != 200*time.Millisecond || cfg.OutboxBatchSize != 50 {
		t.Fatalf("outbox defaults = (%s, %d)", cfg.OutboxPollInterval, cfg.OutboxBatchSize)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("HTTP_ADDR", "127.0.0.1:9090")
	t.Setenv("OUTBOX_POLL_INTERVAL", "350ms")
	t.Setenv("OUTBOX_BATCH_SIZE", "25")
	t.Setenv("CONSUMER_PREFETCH", "7")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9090" ||
		cfg.OutboxPollInterval != 350*time.Millisecond ||
		cfg.OutboxBatchSize != 25 ||
		cfg.ConsumerPrefetch != 7 {
		t.Fatalf("LoadFromEnv() returned unexpected config: %+v", cfg)
	}
}

func TestLoadFromEnvRejectsMalformedValues(t *testing.T) {
	t.Setenv("OUTBOX_POLL_INTERVAL", "soon")
	if _, err := LoadFromEnv(); err == nil || !strings.Contains(err.Error(), "OUTBOX_POLL_INTERVAL") {
		t.Fatalf("LoadFromEnv() error = %v, want invalid OUTBOX_POLL_INTERVAL", err)
	}
}

func TestValidateRejectsInvalidSettings(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DatabaseURL = "http://localhost/db"
	cfg.OutboxBatchSize = 0
	cfg.ConsumerRetryMaxDelay = time.Millisecond

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want validation errors")
	}
	for _, want := range []string{"DATABASE_URL", "OUTBOX_BATCH_SIZE", "CONSUMER_RETRY_MAX_DELAY"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Validate() error %q does not contain %q", err, want)
		}
	}
}

func TestFlagsOverrideEnvironmentDefaults(t *testing.T) {
	cfg := DefaultConfig()
	fs := flag.NewFlagSet("control-plane", flag.ContinueOnError)
	cfg.RegisterFlags(fs)
	if err := fs.Parse([]string{"-http-addr=:9091", "-outbox-batch-size=12"}); err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if cfg.HTTPAddr != ":9091" || cfg.OutboxBatchSize != 12 {
		t.Fatalf("flags did not update config: %+v", cfg)
	}
}
