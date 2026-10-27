package config

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr               string
	DatabaseURL            string
	RabbitMQURL            string
	OutboxPollInterval     time.Duration
	OutboxBatchSize        int
	ShutdownTimeout        time.Duration
	ReadTimeout            time.Duration
	WriteTimeout           time.Duration
	IdleTimeout            time.Duration
	RequestBodyLimit       int64
	ConsumerPrefetch       int
	ConsumerMaxRetries     int
	ConsumerRetryBaseDelay time.Duration
	ConsumerRetryMaxDelay  time.Duration
}

func DefaultConfig() Config {
	return Config{
		HTTPAddr:               ":8080",
		DatabaseURL:            "postgres://tenant:tenant@localhost:5432/tenant_control_plane?sslmode=disable",
		RabbitMQURL:            "amqp://guest:guest@localhost:5672/",
		OutboxPollInterval:     200 * time.Millisecond,
		OutboxBatchSize:        50,
		ShutdownTimeout:        10 * time.Second,
		ReadTimeout:            5 * time.Second,
		WriteTimeout:           10 * time.Second,
		IdleTimeout:            60 * time.Second,
		RequestBodyLimit:       1 << 20,
		ConsumerPrefetch:       10,
		ConsumerMaxRetries:     5,
		ConsumerRetryBaseDelay: 100 * time.Millisecond,
		ConsumerRetryMaxDelay:  5 * time.Second,
	}
}

func LoadFromEnv() (Config, error) {
	cfg := DefaultConfig()
	var err error

	if cfg.HTTPAddr, err = stringFromEnv("HTTP_ADDR", cfg.HTTPAddr); err != nil {
		return Config{}, err
	}
	if cfg.DatabaseURL, err = stringFromEnv("DATABASE_URL", cfg.DatabaseURL); err != nil {
		return Config{}, err
	}
	if cfg.RabbitMQURL, err = stringFromEnv("RABBITMQ_URL", cfg.RabbitMQURL); err != nil {
		return Config{}, err
	}
	if cfg.OutboxPollInterval, err = durationFromEnv("OUTBOX_POLL_INTERVAL", cfg.OutboxPollInterval); err != nil {
		return Config{}, err
	}
	if cfg.OutboxBatchSize, err = intFromEnv("OUTBOX_BATCH_SIZE", cfg.OutboxBatchSize); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = durationFromEnv("SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ReadTimeout, err = durationFromEnv("HTTP_READ_TIMEOUT", cfg.ReadTimeout); err != nil {
		return Config{}, err
	}
	if cfg.WriteTimeout, err = durationFromEnv("HTTP_WRITE_TIMEOUT", cfg.WriteTimeout); err != nil {
		return Config{}, err
	}
	if cfg.IdleTimeout, err = durationFromEnv("HTTP_IDLE_TIMEOUT", cfg.IdleTimeout); err != nil {
		return Config{}, err
	}
	if cfg.RequestBodyLimit, err = int64FromEnv("HTTP_REQUEST_BODY_LIMIT", cfg.RequestBodyLimit); err != nil {
		return Config{}, err
	}
	if cfg.ConsumerPrefetch, err = intFromEnv("CONSUMER_PREFETCH", cfg.ConsumerPrefetch); err != nil {
		return Config{}, err
	}
	if cfg.ConsumerMaxRetries, err = intFromEnv("CONSUMER_MAX_RETRIES", cfg.ConsumerMaxRetries); err != nil {
		return Config{}, err
	}
	if cfg.ConsumerRetryBaseDelay, err = durationFromEnv("CONSUMER_RETRY_BASE_DELAY", cfg.ConsumerRetryBaseDelay); err != nil {
		return Config{}, err
	}
	if cfg.ConsumerRetryMaxDelay, err = durationFromEnv("CONSUMER_RETRY_MAX_DELAY", cfg.ConsumerRetryMaxDelay); err != nil {
		return Config{}, err
	}

	return cfg, cfg.Validate()
}

func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.HTTPAddr, "http-addr", c.HTTPAddr, "HTTP listen address")
	fs.StringVar(&c.DatabaseURL, "database-url", c.DatabaseURL, "PostgreSQL connection URL")
	fs.StringVar(&c.RabbitMQURL, "rabbitmq-url", c.RabbitMQURL, "RabbitMQ connection URL")
	fs.DurationVar(&c.OutboxPollInterval, "outbox-poll-interval", c.OutboxPollInterval, "outbox polling interval")
	fs.IntVar(&c.OutboxBatchSize, "outbox-batch-size", c.OutboxBatchSize, "maximum outbox rows per poll")
	fs.DurationVar(&c.ShutdownTimeout, "shutdown-timeout", c.ShutdownTimeout, "graceful shutdown timeout")
	fs.DurationVar(&c.ReadTimeout, "http-read-timeout", c.ReadTimeout, "HTTP read timeout")
	fs.DurationVar(&c.WriteTimeout, "http-write-timeout", c.WriteTimeout, "HTTP write timeout")
	fs.DurationVar(&c.IdleTimeout, "http-idle-timeout", c.IdleTimeout, "HTTP idle timeout")
	fs.Int64Var(&c.RequestBodyLimit, "http-request-body-limit", c.RequestBodyLimit, "maximum HTTP request body size")
	fs.IntVar(&c.ConsumerPrefetch, "consumer-prefetch", c.ConsumerPrefetch, "maximum unacknowledged broker deliveries")
	fs.IntVar(&c.ConsumerMaxRetries, "consumer-max-retries", c.ConsumerMaxRetries, "maximum retries for transient updates")
	fs.DurationVar(&c.ConsumerRetryBaseDelay, "consumer-retry-base-delay", c.ConsumerRetryBaseDelay, "base delay for consumer retries")
	fs.DurationVar(&c.ConsumerRetryMaxDelay, "consumer-retry-max-delay", c.ConsumerRetryMaxDelay, "maximum delay for consumer retries")
}

func (c Config) Validate() error {
	var errs []error
	if c.HTTPAddr == "" {
		errs = append(errs, errors.New("HTTP_ADDR must not be empty"))
	}
	if err := validateURL("DATABASE_URL", c.DatabaseURL, "postgres", "postgresql"); err != nil {
		errs = append(errs, err)
	}
	if err := validateURL("RABBITMQ_URL", c.RabbitMQURL, "amqp", "amqps"); err != nil {
		errs = append(errs, err)
	}
	if c.OutboxPollInterval <= 0 {
		errs = append(errs, errors.New("OUTBOX_POLL_INTERVAL must be positive"))
	}
	if c.OutboxBatchSize <= 0 {
		errs = append(errs, errors.New("OUTBOX_BATCH_SIZE must be positive"))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT must be positive"))
	}
	if c.ReadTimeout <= 0 {
		errs = append(errs, errors.New("HTTP_READ_TIMEOUT must be positive"))
	}
	if c.WriteTimeout <= 0 {
		errs = append(errs, errors.New("HTTP_WRITE_TIMEOUT must be positive"))
	}
	if c.IdleTimeout <= 0 {
		errs = append(errs, errors.New("HTTP_IDLE_TIMEOUT must be positive"))
	}
	if c.RequestBodyLimit <= 0 {
		errs = append(errs, errors.New("HTTP_REQUEST_BODY_LIMIT must be positive"))
	}
	if c.ConsumerPrefetch <= 0 {
		errs = append(errs, errors.New("CONSUMER_PREFETCH must be positive"))
	}
	if c.ConsumerMaxRetries < 0 {
		errs = append(errs, errors.New("CONSUMER_MAX_RETRIES must not be negative"))
	}
	if c.ConsumerRetryBaseDelay <= 0 {
		errs = append(errs, errors.New("CONSUMER_RETRY_BASE_DELAY must be positive"))
	}
	if c.ConsumerRetryMaxDelay < c.ConsumerRetryBaseDelay {
		errs = append(errs, errors.New("CONSUMER_RETRY_MAX_DELAY must be at least CONSUMER_RETRY_BASE_DELAY"))
	}
	return errors.Join(errs...)
}

func validateURL(name, raw string, schemes ...string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("%s must be a valid URL with a host", name)
	}
	for _, scheme := range schemes {
		if parsed.Scheme == scheme {
			return nil
		}
	}
	return fmt.Errorf("%s has an unsupported URL scheme", name)
}

func stringFromEnv(name, fallback string) (string, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	return value, nil
}

func durationFromEnv(name string, fallback time.Duration) (time.Duration, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", name, err)
	}
	return parsed, nil
}

func intFromEnv(name string, fallback int) (int, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return parsed, nil
}

func int64FromEnv(name string, fallback int64) (int64, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return parsed, nil
}
