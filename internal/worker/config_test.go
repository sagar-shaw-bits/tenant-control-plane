package worker

import (
	"flag"
	"testing"
)

func TestDefaultConfigValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig().Validate() error = %v", err)
	}
	if cfg.MinDelayMS != 500 || cfg.MaxDelayMS != 2000 {
		t.Fatalf("default delays = (%d, %d)", cfg.MinDelayMS, cfg.MaxDelayMS)
	}
}

func TestLoadConfigFromEnvironment(t *testing.T) {
	t.Setenv("FAIL_RATE", "0.75")
	t.Setenv("MIN_DELAY_MS", "10")
	t.Setenv("MAX_DELAY_MS", "20")
	t.Setenv("DUPLICATE_RATE", "0.25")
	t.Setenv("OUT_OF_ORDER", "true")
	t.Setenv("PREFETCH", "5")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.FailRate != 0.75 || cfg.MinDelayMS != 10 || cfg.MaxDelayMS != 20 ||
		cfg.DuplicateRate != 0.25 || !cfg.OutOfOrder || cfg.Prefetch != 5 {
		t.Fatalf("LoadConfig() returned unexpected config: %+v", cfg)
	}
}

func TestWorkerFlagsOverrideConfig(t *testing.T) {
	cfg := DefaultConfig()
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	cfg.RegisterFlags(flags)
	if err := flags.Parse([]string{
		"--fail-rate=1",
		"--min-delay-ms=0",
		"--max-delay-ms=0",
		"--duplicate-rate=1",
		"--out-of-order",
	}); err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if cfg.FailRate != 1 || cfg.MinDelayMS != 0 || cfg.MaxDelayMS != 0 ||
		cfg.DuplicateRate != 1 || !cfg.OutOfOrder {
		t.Fatalf("flags returned unexpected config: %+v", cfg)
	}
}

func TestValidateRejectsInvalidWorkerSettings(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FailRate = 1.1
	cfg.MinDelayMS = 10
	cfg.MaxDelayMS = 9
	cfg.DuplicateRate = -0.1
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want invalid config error")
	}
}
