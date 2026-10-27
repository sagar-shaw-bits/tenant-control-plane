package server

import (
	"context"
	"errors"
	"fmt"
)

type BrokerReadiness interface {
	Connected() bool
}

type DependencyReadiness struct {
	pool   databasePinger
	broker BrokerReadiness
}

type databasePinger interface {
	Ping(context.Context) error
}

func NewDependencyReadiness(pool databasePinger, broker BrokerReadiness) (*DependencyReadiness, error) {
	if pool == nil {
		return nil, errors.New("readiness requires a database pool")
	}
	if broker == nil {
		return nil, errors.New("readiness requires a broker")
	}
	return &DependencyReadiness{pool: pool, broker: broker}, nil
}

func (r *DependencyReadiness) Check(ctx context.Context) error {
	if err := r.pool.Ping(ctx); err != nil {
		return fmt.Errorf("database ping: %w", err)
	}
	if !r.broker.Connected() {
		return errors.New("RabbitMQ is disconnected")
	}
	return nil
}
