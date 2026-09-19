package hatPipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type healthTestConnector struct {
	mu          sync.Mutex
	startCalls  int
	startErrors int
	startErr    error
	paused      int
	resumed     int
}

func (c *healthTestConnector) Start(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.startCalls++
	if c.startCalls <= c.startErrors {
		return c.startErr
	}
	return nil
}

func (c *healthTestConnector) Pause(context.Context) error {
	c.mu.Lock()
	c.paused++
	c.mu.Unlock()
	return nil
}

func (c *healthTestConnector) Resume(context.Context) error {
	c.mu.Lock()
	c.resumed++
	c.mu.Unlock()
	return nil
}

func (c *healthTestConnector) Stop(context.Context) error { return nil }

func (c *healthTestConnector) counts() (int, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.startCalls, c.paused, c.resumed
}

func TestStartWithHealthPolicyRetriesWithBoundedBackoff(t *testing.T) {
	startErr := errors.New("temporary source outage")
	connector := &healthTestConnector{startErrors: 2, startErr: startErr}
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatalf("NewConnectorRegistry() error = %v", err)
	}
	if err := registry.Register("orders", connector); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	status, err := registry.StartWithHealthPolicy(context.Background(), "orders", ConnectorHealthPolicy{
		MaxAttempts:         3,
		InitialBackoff:      time.Nanosecond,
		MaxBackoff:          time.Nanosecond,
		QuarantineOnFailure: true,
	})
	if err != nil {
		t.Fatalf("StartWithHealthPolicy() error = %v", err)
	}
	if status.State != ConnectorHealthy || status.Attempts != 3 {
		t.Fatalf("health status = %#v, want healthy after three attempts", status)
	}
	if status.LastError != "" || !status.NextRetryAt.IsZero() {
		t.Fatalf("successful health status = %#v, want no error or pending retry", status)
	}
	if lifecycle, ok := registry.Status("orders"); !ok || lifecycle.State != ConnectorRunning {
		t.Fatalf("lifecycle status = %#v, want running", lifecycle)
	}
	starts, _, _ := connector.counts()
	if starts != 3 {
		t.Fatalf("start calls = %d, want 3", starts)
	}
}

func TestStartWithHealthPolicyQuarantinesAndRequiresRelease(t *testing.T) {
	startErr := errors.New("permanent source outage")
	connector := &healthTestConnector{startErrors: 10, startErr: startErr}
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatalf("NewConnectorRegistry() error = %v", err)
	}
	if err := registry.Register("payments", connector); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	status, err := registry.StartWithHealthPolicy(context.Background(), "payments", ConnectorHealthPolicy{
		MaxAttempts:         2,
		InitialBackoff:      time.Nanosecond,
		MaxBackoff:          time.Nanosecond,
		QuarantineOnFailure: true,
	})
	if !errors.Is(err, startErr) {
		t.Fatalf("failure = %v, want source error", err)
	}
	if status.State != ConnectorQuarantined || status.Attempts != 2 || status.LastError != startErr.Error() {
		t.Fatalf("quarantine status = %#v, want two attempts and source error", status)
	}
	if _, err := registry.StartWithHealthPolicy(context.Background(), "payments", ConnectorHealthPolicy{MaxAttempts: 1}); !errors.Is(err, ErrConnectorQuarantined) {
		t.Fatalf("retry while quarantined = %v, want ErrConnectorQuarantined", err)
	}
	starts, _, _ := connector.counts()
	if starts != 2 {
		t.Fatalf("start calls after quarantine = %d, want 2", starts)
	}

	if err := registry.ReleaseConnectorQuarantine("payments"); err != nil {
		t.Fatalf("ReleaseConnectorQuarantine() error = %v", err)
	}
	connector.mu.Lock()
	connector.startErrors = connector.startCalls
	connector.mu.Unlock()
	status, err = registry.StartWithHealthPolicy(context.Background(), "payments", ConnectorHealthPolicy{MaxAttempts: 1})
	if err != nil {
		t.Fatalf("start after release = %v", err)
	}
	if status.State != ConnectorHealthy || status.Attempts != 1 {
		t.Fatalf("post-release status = %#v, want healthy after one attempt", status)
	}
}

func TestQuarantineConnectorPausesRunningConnector(t *testing.T) {
	connector := &healthTestConnector{}
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatalf("NewConnectorRegistry() error = %v", err)
	}
	if err := registry.Register("inventory", connector); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := registry.Start(context.Background(), "inventory"); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	status, err := registry.QuarantineConnector(context.Background(), "inventory", "operator requested isolation")
	if err != nil {
		t.Fatalf("QuarantineConnector() error = %v", err)
	}
	if status.State != ConnectorQuarantined || status.LastError != "operator requested isolation" {
		t.Fatalf("quarantine status = %#v, want operator reason", status)
	}
	if lifecycle, ok := registry.Status("inventory"); !ok || lifecycle.State != ConnectorPaused {
		t.Fatalf("lifecycle status = %#v, want paused", lifecycle)
	}
	_, paused, _ := connector.counts()
	if paused != 1 {
		t.Fatalf("pause calls = %d, want 1", paused)
	}
}

func TestStartWithHealthPolicyHonorsCancellationAndRetryability(t *testing.T) {
	startErr := errors.New("retryable source error")
	connector := &healthTestConnector{startErrors: 10, startErr: startErr}
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatalf("NewConnectorRegistry() error = %v", err)
	}
	if err := registry.Register("events", connector); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	status, err := registry.StartWithHealthPolicy(ctx, "events", ConnectorHealthPolicy{
		MaxAttempts:    5,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		Retryable: func(error) bool {
			cancel()
			return true
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v, want context.Canceled", err)
	}
	if status.State != ConnectorHealthFailed || status.Attempts != 1 {
		t.Fatalf("cancellation status = %#v, want one failed attempt", status)
	}
	if status.LastError != startErr.Error() {
		t.Fatalf("cancellation last error = %q, want source error %q", status.LastError, startErr.Error())
	}
	starts, _, _ := connector.counts()
	if starts != 1 {
		t.Fatalf("start calls after cancellation = %d, want 1", starts)
	}
}

func TestConnectorHealthPolicyValidationAndSnapshot(t *testing.T) {
	if _, err := NewConnectorRegistry(ConnectorRegistryOptions{}); err != nil {
		t.Fatalf("NewConnectorRegistry() error = %v", err)
	}
	if policy := DefaultConnectorHealthPolicy(); policy.MaxAttempts <= 0 || policy.InitialBackoff <= 0 || policy.MaxBackoff < policy.InitialBackoff {
		t.Fatalf("default health policy = %#v, want bounded positive values", policy)
	}
	connector := &healthTestConnector{}
	registry, _ := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err := registry.Register("one", connector); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, err := registry.StartWithHealthPolicy(context.Background(), "one", ConnectorHealthPolicy{MaxAttempts: -1}); !errors.Is(err, ErrConnectorHealthPolicyInvalid) {
		t.Fatalf("negative attempts error = %v, want policy validation", err)
	}
	if _, err := registry.StartWithHealthPolicy(context.Background(), "one", ConnectorHealthPolicy{InitialBackoff: -time.Second}); !errors.Is(err, ErrConnectorHealthPolicyInvalid) {
		t.Fatalf("negative backoff error = %v, want policy validation", err)
	}
	snapshot := registry.HealthSnapshot()
	if len(snapshot) != 1 || snapshot[0].ID != "one" || snapshot[0].State != ConnectorUnknown {
		t.Fatalf("health snapshot = %#v, want one unknown connector", snapshot)
	}
}
