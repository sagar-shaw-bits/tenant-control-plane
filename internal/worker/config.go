package worker

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
)

type Config struct {
	FailRate      float64
	MinDelayMS    int64
	MaxDelayMS    int64
	DuplicateRate float64
	OutOfOrder    bool
	Prefetch      int
}

func DefaultConfig() Config {
	return Config{
		MinDelayMS: 500,
		MaxDelayMS: 2000,
		Prefetch:   10,
	}
}

func LoadConfig() (Config, error) {
	cfg := DefaultConfig()
	var err error
	if cfg.FailRate, err = floatFromEnv("FAIL_RATE", cfg.FailRate); err != nil {
		return Config{}, err
	}
	if cfg.MinDelayMS, err = int64FromEnv("MIN_DELAY_MS", cfg.MinDelayMS); err != nil {
		return Config{}, err
	}
	if cfg.MaxDelayMS, err = int64FromEnv("MAX_DELAY_MS", cfg.MaxDelayMS); err != nil {
		return Config{}, err
	}
	if cfg.DuplicateRate, err = floatFromEnv("DUPLICATE_RATE", cfg.DuplicateRate); err != nil {
		return Config{}, err
	}
	if cfg.OutOfOrder, err = boolFromEnv("OUT_OF_ORDER", cfg.OutOfOrder); err != nil {
		return Config{}, err
	}
	if cfg.Prefetch, err = intFromEnv("PREFETCH", cfg.Prefetch); err != nil {
		return Config{}, err
	}
	return cfg, cfg.Validate()
}

func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.Float64Var(&c.FailRate, "fail-rate", c.FailRate, "probability of a failed task (0..1)")
	fs.Int64Var(&c.MinDelayMS, "min-delay-ms", c.MinDelayMS, "minimum simulated task delay in milliseconds")
	fs.Int64Var(&c.MaxDelayMS, "max-delay-ms", c.MaxDelayMS, "maximum simulated task delay in milliseconds")
	fs.Float64Var(&c.DuplicateRate, "duplicate-rate", c.DuplicateRate, "probability of publishing duplicate updates (0..1)")
	fs.BoolVar(&c.OutOfOrder, "out-of-order", c.OutOfOrder, "publish terminal update before in_progress")
	fs.IntVar(&c.Prefetch, "prefetch", c.Prefetch, "maximum unacknowledged task deliveries")
}

func (c Config) Validate() error {
	var errs []error
	if math.IsNaN(c.FailRate) || c.FailRate < 0 || c.FailRate > 1 {
		errs = append(errs, errors.New("FAIL_RATE must be between 0 and 1"))
	}
	if c.MinDelayMS < 0 {
		errs = append(errs, errors.New("MIN_DELAY_MS must not be negative"))
	}
	if c.MaxDelayMS < c.MinDelayMS {
		errs = append(errs, errors.New("MAX_DELAY_MS must be greater than or equal to MIN_DELAY_MS"))
	}
	if c.MaxDelayMS > int64(math.MaxInt64/int64(time.Millisecond)) {
		errs = append(errs, errors.New("MAX_DELAY_MS is too large"))
	}
	if math.IsNaN(c.DuplicateRate) || c.DuplicateRate < 0 || c.DuplicateRate > 1 {
		errs = append(errs, errors.New("DUPLICATE_RATE must be between 0 and 1"))
	}
	if c.Prefetch <= 0 {
		errs = append(errs, errors.New("PREFETCH must be positive"))
	}
	return errors.Join(errs...)
}

func floatFromEnv(name string, fallback float64) (float64, error) {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number: %w", name, err)
	}
	return value, nil
}

func int64FromEnv(name string, fallback int64) (int64, error) {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return value, nil
}

func intFromEnv(name string, fallback int) (int, error) {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return value, nil
}

func boolFromEnv(name string, fallback bool) (bool, error) {
	raw, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false: %w", name, err)
	}
	return value, nil
}
