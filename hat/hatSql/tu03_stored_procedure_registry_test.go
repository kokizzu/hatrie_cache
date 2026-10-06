package hatSql

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestTU03StoredProcedureRegistryVersionedAuthorizationAndInvocation(t *testing.T) {
	var authorized StoredProcedureAuthorization
	registry, err := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{
		MaxProcedures:  2,
		MaxArguments:   2,
		MaxInputBytes:  128,
		MaxOutputBytes: 128,
		Authorize: func(request StoredProcedureAuthorization) bool {
			authorized = request
			return request.Principal == "worker" && request.RequiredCapability == "orders.execute"
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := registry.Register(StoredProcedure{
		Name:               "sum",
		Version:            "v1",
		RequiredCapability: "orders.execute",
		Execute: func(_ context.Context, arguments []interface{}) (interface{}, error) {
			return arguments[0].(int64) + arguments[1].(int64), nil
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Generation != 1 || metadata.Name != "sum" || metadata.Version != "v1" {
		t.Fatalf("initial metadata = %#v", metadata)
	}
	value, err := registry.Invoke(context.Background(), "worker", "sum", []interface{}{int64(2), int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	if value != int64(5) {
		t.Fatalf("Invoke() = %#v, want 5", value)
	}
	if authorized.Principal != "worker" || authorized.Name != "sum" || authorized.Version != "v1" {
		t.Fatalf("authorization request = %#v", authorized)
	}

	if _, err := registry.Register(StoredProcedure{
		Name:               "sum",
		Version:            "v2",
		RequiredCapability: "orders.execute",
		Execute:            func(context.Context, []interface{}) (interface{}, error) { return int64(8), nil },
	}, "stale"); !errors.Is(err, ErrStoredProcedureVersionConflict) {
		t.Fatalf("stale replacement error = %v, want version conflict", err)
	}
	if _, err := registry.Register(StoredProcedure{
		Name:               "sum",
		Version:            "v2",
		RequiredCapability: "orders.execute",
		Execute:            func(context.Context, []interface{}) (interface{}, error) { return int64(8), nil },
	}, "v1"); err != nil {
		t.Fatal(err)
	}
	value, err = registry.Invoke(context.Background(), "worker", "sum", nil)
	if err != nil {
		t.Fatal(err)
	}
	if value != int64(8) {
		t.Fatalf("replacement Invoke() = %#v, want 8", value)
	}
	snapshot := registry.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Version != "v2" || snapshot[0].Generation != 2 {
		t.Fatalf("Snapshot() = %#v", snapshot)
	}
}

func TestTU03StoredProcedureRegistryFailsClosedAndIsolatesPanics(t *testing.T) {
	registry, err := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{
		MaxArguments:   1,
		MaxInputBytes:  16,
		MaxOutputBytes: 16,
		Authorize: func(StoredProcedureAuthorization) bool {
			return true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	register := func(name string, execute StoredProcedureHandler) {
		t.Helper()
		if _, err := registry.Register(StoredProcedure{Name: name, Version: "v1", Execute: execute}, ""); err != nil {
			t.Fatal(err)
		}
	}
	register("echo", func(_ context.Context, arguments []interface{}) (interface{}, error) { return arguments[0], nil })
	register("panic", func(context.Context, []interface{}) (interface{}, error) { panic("boom") })
	register("large", func(context.Context, []interface{}) (interface{}, error) { return strings.Repeat("x", 32), nil })

	if _, err := registry.Invoke(context.Background(), "worker", "echo", []interface{}{strings.Repeat("x", 32)}); !errors.Is(err, ErrStoredProcedureInputLimit) {
		t.Fatalf("input limit error = %v", err)
	}
	if _, err := registry.Invoke(context.Background(), "worker", "echo", []interface{}{1, 2}); !errors.Is(err, ErrStoredProcedureArgumentLimit) {
		t.Fatalf("argument limit error = %v", err)
	}
	if _, err := registry.Invoke(context.Background(), "worker", "large", nil); !errors.Is(err, ErrStoredProcedureOutputLimit) {
		t.Fatalf("output limit error = %v", err)
	}
	if _, err := registry.Invoke(context.Background(), "worker", "panic", nil); !errors.Is(err, ErrStoredProcedurePanic) {
		t.Fatalf("panic error = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Invoke(canceled, "worker", "echo", []interface{}{"ok"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}

	noAuth, err := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := noAuth.Register(StoredProcedure{Name: "echo", Version: "v1", Execute: func(context.Context, []interface{}) (interface{}, error) { return nil, nil }}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := noAuth.Invoke(context.Background(), "worker", "echo", nil); !errors.Is(err, ErrStoredProcedureAuthorizationRequired) {
		t.Fatalf("missing authorizer error = %v", err)
	}
}

func TestTU03StoredProcedureRegistryRejectsInvalidAndUntrustedMetadata(t *testing.T) {
	registry, err := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{Authorize: func(StoredProcedureAuthorization) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(StoredProcedure{Name: "", Version: "v1", Execute: func(context.Context, []interface{}) (interface{}, error) { return nil, nil }}, ""); !errors.Is(err, ErrStoredProcedureInvalid) {
		t.Fatalf("invalid registration error = %v", err)
	}
	if _, err := registry.Register(StoredProcedure{Name: "missing-handler", Version: "v1"}, ""); !errors.Is(err, ErrStoredProcedureInvalid) {
		t.Fatalf("missing handler error = %v", err)
	}
	if _, err := registry.Register(StoredProcedure{Name: "echo", Version: "v1", Execute: func(context.Context, []interface{}) (interface{}, error) { return nil, nil }}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(StoredProcedure{Name: "echo", Version: "v1", Execute: func(context.Context, []interface{}) (interface{}, error) { return nil, nil }}, "v1"); !errors.Is(err, ErrStoredProcedureVersionUnchanged) {
		t.Fatalf("unchanged replacement error = %v", err)
	}
}

func TestTU03StoredProcedureRegistryConcurrentInvokeAndMetadataReads(t *testing.T) {
	registry, err := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{
		Authorize: func(StoredProcedureAuthorization) bool { return true },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(StoredProcedure{
		Name:    "constant",
		Version: "v1",
		Execute: func(context.Context, []interface{}) (interface{}, error) { return int64(1), nil },
	}, ""); err != nil {
		t.Fatal(err)
	}
	errorsFound := make(chan error, 16)
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 64; iteration++ {
				if value, err := registry.Invoke(context.Background(), "worker", "constant", nil); err != nil {
					errorsFound <- err
				} else if value != int64(1) {
					errorsFound <- errors.New("unexpected concurrent procedure result")
				}
				if _, ok := registry.Resolve("constant"); !ok {
					errorsFound <- errors.New("concurrent procedure lookup failed")
				}
				if snapshot := registry.Snapshot(); len(snapshot) != 1 {
					errorsFound <- errors.New("concurrent procedure snapshot changed")
				}
			}
		}()
	}
	group.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
}
