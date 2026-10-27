package server

import (
	"context"
	"errors"
	"testing"
)

type brokerReadinessFake struct {
	connected bool
}

func (f brokerReadinessFake) Connected() bool { return f.connected }

type databasePingerFake struct {
	err error
}

func (f databasePingerFake) Ping(context.Context) error { return f.err }

func TestNewDependencyReadinessRequiresDependencies(t *testing.T) {
	if _, err := NewDependencyReadiness(nil, brokerReadinessFake{connected: true}); err == nil {
		t.Fatal("NewDependencyReadiness(nil, ...) error = nil")
	}
	if _, err := NewDependencyReadiness(databasePingerFake{}, nil); err == nil {
		t.Fatal("NewDependencyReadiness(..., nil) error = nil")
	}
}

func TestDependencyReadinessRejectsDisconnectedBroker(t *testing.T) {
	readiness, err := NewDependencyReadiness(databasePingerFake{}, brokerReadinessFake{})
	if err != nil {
		t.Fatalf("NewDependencyReadiness() error = %v", err)
	}
	if err := readiness.Check(context.Background()); err == nil {
		t.Fatal("Check() error = nil, want failure")
	}
}

func TestDependencyReadinessChecksDatabaseFirst(t *testing.T) {
	readiness, err := NewDependencyReadiness(databasePingerFake{err: errors.New("db down")}, brokerReadinessFake{connected: true})
	if err != nil {
		t.Fatalf("NewDependencyReadiness() error = %v", err)
	}
	if err := readiness.Check(context.Background()); err == nil {
		t.Fatal("Check() error = nil, want database failure")
	}
}
