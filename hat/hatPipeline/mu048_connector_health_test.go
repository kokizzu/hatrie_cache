package hatPipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type mu048FlakyConnector struct {
	failures atomic.Int32
	calls    atomic.Int32
}

func (connector *mu048FlakyConnector) Start(context.Context) error {
	call := connector.calls.Add(1)
	if call <= connector.failures.Load() {
		return errors.New("temporary connector failure")
	}
	return nil
}

func (*mu048FlakyConnector) Pause(context.Context) error  { return nil }
func (*mu048FlakyConnector) Resume(context.Context) error { return nil }
func (*mu048FlakyConnector) Stop(context.Context) error   { return nil }

func TestMU048ConnectorHealthRetriesAndRecovers(t *testing.T) {
	connector := &mu048FlakyConnector{}
	connector.failures.Store(2)
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders", connector); err != nil {
		t.Fatal(err)
	}
	result, err := registry.StartWithHealthPolicy(context.Background(), "orders", ConnectorHealthPolicy{
		MaxAttempts:     3,
		QuarantineAfter: 3,
		InitialBackoff:  time.Nanosecond,
		MaxBackoff:      time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempts != 3 || result.Failures != 2 || !result.Recovered || result.Quarantined {
		t.Fatalf("health result = %#v, want three attempts, two failures, recovered", result)
	}
	status, ok := registry.Status("orders")
	if !ok || status.State != ConnectorRunning {
		t.Fatalf("status = %#v/%t, want running", status, ok)
	}
}

func TestMU048ConnectorHealthQuarantinesAfterBound(t *testing.T) {
	connector := &mu048FlakyConnector{}
	connector.failures.Store(10)
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("payments", connector); err != nil {
		t.Fatal(err)
	}
	result, err := registry.StartWithHealthPolicy(context.Background(), "payments", ConnectorHealthPolicy{
		MaxAttempts:     5,
		QuarantineAfter: 2,
		InitialBackoff:  time.Nanosecond,
		MaxBackoff:      time.Nanosecond,
	})
	if !errors.Is(err, ErrConnectorHealthQuarantined) {
		t.Fatalf("health error = %v, want quarantine", err)
	}
	if result.Attempts != 2 || result.Failures != 2 || !result.Quarantined || result.Recovered {
		t.Fatalf("health result = %#v, want bounded quarantine", result)
	}
	if calls := connector.calls.Load(); calls != 2 {
		t.Fatalf("connector calls = %d, want 2", calls)
	}
	status, ok := registry.Status("payments")
	if !ok || status.State != ConnectorFailed {
		t.Fatalf("status = %#v/%t, want failed quarantine state", status, ok)
	}
}

func TestMU048ConnectorHealthCancellationAndValidation(t *testing.T) {
	for _, policy := range []ConnectorHealthPolicy{
		{MaxAttempts: -1},
		{QuarantineAfter: -1},
		{InitialBackoff: -time.Nanosecond},
		{MaxBackoff: -time.Nanosecond},
		{MaxAttempts: 1, QuarantineAfter: 2},
		{MaxAttempts: 1, InitialBackoff: time.Second, MaxBackoff: time.Nanosecond},
	} {
		registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Register("source", &mu048FlakyConnector{}); err != nil {
			t.Fatal(err)
		}
		if _, err := registry.StartWithHealthPolicy(context.Background(), "source", policy); !errors.Is(err, ErrConnectorHealthPolicyInvalid) {
			t.Fatalf("policy %#v error = %v, want invalid policy", policy, err)
		}
	}

	connector := &mu048FlakyConnector{}
	connector.failures.Store(10)
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("source", connector); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	connectorCancel := &mu048CancelOnStartConnector{cancel: cancel}
	if err := registry.Unregister("source"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("source", connectorCancel); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = registry.StartWithHealthPolicy(ctx, "source", ConnectorHealthPolicy{
		MaxAttempts:     3,
		QuarantineAfter: 3,
		InitialBackoff:  time.Second,
		MaxBackoff:      time.Second,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled health error = %v, want context canceled", err)
	}
	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("canceled retry waited %s, want immediate cancellation", elapsed)
	}
}

type mu048CancelOnStartConnector struct {
	cancel context.CancelFunc
}

func (connector *mu048CancelOnStartConnector) Start(context.Context) error {
	connector.cancel()
	return errors.New("temporary connector failure")
}

func (*mu048CancelOnStartConnector) Pause(context.Context) error  { return nil }
func (*mu048CancelOnStartConnector) Resume(context.Context) error { return nil }
func (*mu048CancelOnStartConnector) Stop(context.Context) error   { return nil }
