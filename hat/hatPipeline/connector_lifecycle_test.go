package hatPipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type lifecycleTestConnector struct {
	mu         sync.Mutex
	calls      []string
	startError error
	maxActive  int
	active     int
}

func (c *lifecycleTestConnector) enter(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, name)
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	defer func() { c.active-- }()
	return nil
}

func (c *lifecycleTestConnector) Start(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "start")
	if c.startError != nil {
		return c.startError
	}
	return nil
}

func (c *lifecycleTestConnector) Pause(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "pause")
	return nil
}

func (c *lifecycleTestConnector) Resume(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "resume")
	return nil
}

func (c *lifecycleTestConnector) Stop(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, "stop")
	return nil
}

func (c *lifecycleTestConnector) callNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func TestConnectorRegistryLifecycleAndSnapshot(t *testing.T) {
	ctx := context.Background()
	c := &lifecycleTestConnector{}
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{HistoryLimit: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders-eu", c); err != nil {
		t.Fatal(err)
	}

	assertConnectorState(t, registry, "orders-eu", ConnectorCreated)
	if err := registry.Start(ctx, "orders-eu"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Pause(ctx, "orders-eu"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Resume(ctx, "orders-eu"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Stop(ctx, "orders-eu"); err != nil {
		t.Fatal(err)
	}

	status, ok := registry.Status("orders-eu")
	if !ok {
		t.Fatal("missing connector status")
	}
	if status.State != ConnectorStopped || status.Generation != 4 {
		t.Fatalf("unexpected status: %+v", status)
	}
	if status.LastError != "" {
		t.Fatalf("unexpected last error: %q", status.LastError)
	}
	if got, want := c.callNames(), []string{"start", "pause", "resume", "stop"}; !equalStrings(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}

	snapshot := registry.Snapshot()
	if len(snapshot) != 1 || snapshot[0].ID != "orders-eu" {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	events := registry.Events("orders-eu")
	if len(events) != 4 {
		t.Fatalf("event count = %d, want 4", len(events))
	}
	if events[0].From != ConnectorCreated || events[0].To != ConnectorRunning {
		t.Fatalf("first event = %+v", events[0])
	}
}

func TestConnectorRegistryFailureCanRecover(t *testing.T) {
	ctx := context.Background()
	startError := errors.New("upstream unavailable")
	c := &lifecycleTestConnector{startError: startError}
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("payments", c); err != nil {
		t.Fatal(err)
	}
	if err := registry.Start(ctx, "payments"); !errors.Is(err, startError) {
		t.Fatalf("start error = %v, want %v", err, startError)
	}
	assertConnectorState(t, registry, "payments", ConnectorFailed)
	status, _ := registry.Status("payments")
	if status.LastError != startError.Error() {
		t.Fatalf("last error = %q, want %q", status.LastError, startError)
	}

	c.startError = nil
	if err := registry.Start(ctx, "payments"); err != nil {
		t.Fatal(err)
	}
	status, _ = registry.Status("payments")
	if status.State != ConnectorRunning || status.LastError != "" {
		t.Fatalf("recovered status = %+v", status)
	}
}

func TestConnectorRegistryInvalidTransitionsAndDuplicateIDs(t *testing.T) {
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c := &lifecycleTestConnector{}
	if err := registry.Register("one", c); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(registry.Register("one", c), ErrConnectorAlreadyRegistered) {
		t.Fatal("duplicate registration did not fail")
	}
	if !errors.Is(registry.Pause(context.Background(), "one"), ErrConnectorInvalidTransition) {
		t.Fatal("pause from created did not fail")
	}
	if !errors.Is(registry.Start(context.Background(), "missing"), ErrConnectorNotFound) {
		t.Fatal("missing connector did not fail")
	}
	if !errors.Is(registry.Register("", c), ErrConnectorIDEmpty) {
		t.Fatal("empty connector ID did not fail")
	}
	if err := registry.Unregister("one"); err != nil {
		t.Fatal("created connector did not unregister:", err)
	}
	if _, ok := registry.Status("one"); ok {
		t.Fatal("unregistered connector still has status")
	}
	if !errors.Is(registry.Unregister("one"), ErrConnectorNotFound) {
		t.Fatal("second unregister did not fail")
	}
}

func TestConnectorRegistryDoesNotDropActiveConnector(t *testing.T) {
	ctx := context.Background()
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	c := &lifecycleTestConnector{}
	if err := registry.Register("active", c); err != nil {
		t.Fatal(err)
	}
	if err := registry.Start(ctx, "active"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(registry.Unregister("active"), ErrConnectorInvalidTransition) {
		t.Fatal("active connector was unregistered")
	}
	if err := registry.Stop(ctx, "active"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Unregister("active"); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorRegistryOptionValidation(t *testing.T) {
	if _, err := NewConnectorRegistry(ConnectorRegistryOptions{HistoryLimit: -1}); !errors.Is(err, ErrConnectorHistoryLimitInvalid) {
		t.Fatalf("negative history limit error = %v", err)
	}
	if _, err := NewConnectorRegistry(ConnectorRegistryOptions{HistoryLimit: maxConnectorHistoryLimit + 1}); !errors.Is(err, ErrConnectorHistoryLimitInvalid) {
		t.Fatalf("oversized history limit error = %v", err)
	}
	if err := (&ConnectorRegistry{}).Register("nil", nil); !errors.Is(err, ErrConnectorNil) {
		t.Fatalf("nil connector error = %v", err)
	}
}

func TestConnectorRegistryHistoryIsBoundedAndCloseStopsConnectors(t *testing.T) {
	ctx := context.Background()
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{HistoryLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	c := &lifecycleTestConnector{}
	if err := registry.Register("cache", c); err != nil {
		t.Fatal(err)
	}
	if err := registry.Start(ctx, "cache"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Pause(ctx, "cache"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Resume(ctx, "cache"); err != nil {
		t.Fatal(err)
	}
	events := registry.Events("cache")
	if got := len(events); got != 2 {
		t.Fatalf("bounded event count = %d, want 2", got)
	}
	if events[0].Sequence != 2 || events[1].Sequence != 3 {
		t.Fatalf("bounded events = %+v, want sequences 2 and 3", events)
	}
	if err := registry.Close(ctx); err != nil {
		t.Fatal(err)
	}
	assertConnectorState(t, registry, "cache", ConnectorStopped)
	if !errors.Is(registry.Register("after-close", c), ErrConnectorRegistryClosed) {
		t.Fatal("register after close did not fail")
	}
	if !errors.Is(registry.Start(ctx, "cache"), ErrConnectorRegistryClosed) {
		t.Fatal("start after close did not fail")
	}
}

func TestConnectorRegistryCallbackContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &lifecycleTestConnector{}
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("cancelled", c); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := registry.Start(ctx, "cancelled"); !errors.Is(err, context.Canceled) {
		t.Fatalf("start error = %v, want context cancellation", err)
	}
	status, _ := registry.Status("cancelled")
	if status.State != ConnectorFailed {
		t.Fatalf("cancelled status = %+v", status)
	}
}

func TestConnectorRegistryStatusCopiesTimeAndEvents(t *testing.T) {
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("copy", &lifecycleTestConnector{}); err != nil {
		t.Fatal(err)
	}
	status, _ := registry.Status("copy")
	if status.UpdatedAt.IsZero() {
		t.Fatal("status timestamp is zero")
	}
	if time.Since(status.UpdatedAt) < 0 {
		t.Fatal("status timestamp is in the future")
	}
	events := registry.Events("copy")
	if events == nil {
		t.Fatal("events should return an empty non-nil slice")
	}
	events = append(events, ConnectorEvent{})
	if got := len(registry.Events("copy")); got != 0 {
		t.Fatalf("event copy changed registry, count = %d", got)
	}
}

func assertConnectorState(t *testing.T, registry *ConnectorRegistry, id string, want ConnectorState) {
	t.Helper()
	status, ok := registry.Status(id)
	if !ok {
		t.Fatalf("missing status for %q", id)
	}
	if status.State != want {
		t.Fatalf("state for %q = %s, want %s", id, status.State, want)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
