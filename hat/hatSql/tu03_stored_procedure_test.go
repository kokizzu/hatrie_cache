package hatSql

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

func TestStoredProcedureRegistryVersionedAuthorizedCall(t *testing.T) {
	var authorized int32
	registry := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{
		Authorize: func(_ context.Context, request StoredProcedureRequest) error {
			atomic.AddInt32(&authorized, 1)
			if request.Name != "orders" || request.Version != "v1" || len(request.Arguments) != 1 {
				t.Fatalf("authorization request = %#v", request)
			}
			request.Arguments[0] = int64(99)
			return nil
		},
	})
	if err := registry.Register(StoredProcedureDefinition{
		Name:    " Orders ",
		Version: "v1",
		Execute: func(_ context.Context, request StoredProcedureRequest) (StoredProcedureResponse, error) {
			if request.Arguments[0] != int64(7) {
				return StoredProcedureResponse{}, errors.New("authorization mutated execution arguments")
			}
			request.Arguments[0] = int64(8)
			return StoredProcedureResponse{Values: []interface{}{request.Arguments[0]}}, nil
		},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	input := []interface{}{int64(7)}
	values, err := registry.Call(context.Background(), " ORDERS ", "v1", input)
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if len(values) != 1 || values[0] != int64(8) || input[0] != int64(7) {
		t.Fatalf("Call() = %#v, input = %#v", values, input)
	}
	if atomic.LoadInt32(&authorized) != 1 {
		t.Fatalf("authorization calls = %d, want 1", authorized)
	}
	if got := registry.Versions("orders"); len(got) != 1 || got[0] != "v1" {
		t.Fatalf("Versions() = %#v", got)
	}
}

func TestStoredProcedureRegistryRejectsDuplicatesAndUnknownCalls(t *testing.T) {
	registry := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{})
	definition := StoredProcedureDefinition{Name: "orders", Version: "v1", Execute: func(context.Context, StoredProcedureRequest) (StoredProcedureResponse, error) {
		return StoredProcedureResponse{}, nil
	}}
	if err := registry.Register(definition); err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	if err := registry.Register(definition); !errors.Is(err, ErrStoredProcedureExists) {
		t.Fatalf("duplicate Register() error = %v, want %v", err, ErrStoredProcedureExists)
	}
	if _, err := registry.Call(context.Background(), "missing", "v1", nil); !errors.Is(err, ErrStoredProcedureNotFound) {
		t.Fatalf("unknown Call() error = %v, want %v", err, ErrStoredProcedureNotFound)
	}
	for _, invalid := range []StoredProcedureDefinition{
		{Version: "v1", Execute: definition.Execute},
		{Name: "orders", Execute: definition.Execute},
		{Name: "orders", Version: "v1"},
	} {
		if err := registry.Register(invalid); !errors.Is(err, ErrStoredProcedureInvalid) {
			t.Fatalf("invalid Register(%#v) error = %v, want %v", invalid, err, ErrStoredProcedureInvalid)
		}
	}
}

func TestStoredProcedureRegistryIsolatesPanicAndAuthorizationError(t *testing.T) {
	denied := errors.New("not allowed")
	registry := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{
		Authorize: func(context.Context, StoredProcedureRequest) error { return denied },
	})
	if err := registry.Register(StoredProcedureDefinition{
		Name: "denied", Version: "v1",
		Execute: func(context.Context, StoredProcedureRequest) (StoredProcedureResponse, error) {
			panic("must not run")
		},
	}); err != nil {
		t.Fatalf("Register(denied) error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "denied", "v1", nil); !errors.Is(err, denied) {
		t.Fatalf("authorization error = %v, want %v", err, denied)
	}

	panicRegistry := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{})
	if err := panicRegistry.Register(StoredProcedureDefinition{
		Name: "panic", Version: "v1",
		Execute: func(context.Context, StoredProcedureRequest) (StoredProcedureResponse, error) {
			panic("secret panic payload")
		},
	}); err != nil {
		t.Fatalf("Register(panic) error = %v", err)
	}
	_, err := panicRegistry.Call(context.Background(), "panic", "v1", nil)
	if !errors.Is(err, ErrStoredProcedurePanic) {
		t.Fatalf("panic Call() error = %v, want %v", err, ErrStoredProcedurePanic)
	}
	if strings.Contains(err.Error(), "secret panic payload") {
		t.Fatalf("panic payload leaked in error %q", err)
	}
}

func TestStoredProcedureRegistryBoundsArgumentsAndResults(t *testing.T) {
	registry := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{MaxArguments: 1, MaxResults: 1})
	if err := registry.Register(StoredProcedureDefinition{
		Name: "two", Version: "v1",
		Execute: func(context.Context, StoredProcedureRequest) (StoredProcedureResponse, error) {
			return StoredProcedureResponse{Values: []interface{}{1, 2}}, nil
		},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "two", "v1", []interface{}{1, 2}); !errors.Is(err, ErrStoredProcedureArgumentLimit) {
		t.Fatalf("argument limit error = %v, want %v", err, ErrStoredProcedureArgumentLimit)
	}
	if _, err := registry.Call(context.Background(), "two", "v1", []interface{}{1}); !errors.Is(err, ErrStoredProcedureResultLimit) {
		t.Fatalf("result limit error = %v, want %v", err, ErrStoredProcedureResultLimit)
	}
}
