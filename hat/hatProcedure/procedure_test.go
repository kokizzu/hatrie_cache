package hatProcedure

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestRegistryRequiresExplicitAuthorization(t *testing.T) {
	registry, err := NewRegistry(Options{})
	if err != nil {
		t.Fatalf("NewRegistry returned error: %v", err)
	}
	var called atomic.Int32
	if err := registry.Register(Definition{
		Name:    "echo",
		Version: 1,
		Handler: func(context.Context, []byte) ([]byte, error) { called.Add(1); return nil, nil },
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	_, err = registry.Invoke(context.Background(), Call{Name: "echo", Payload: []byte("x")})
	if !errors.Is(err, ErrAuthorizationRequired) {
		t.Fatalf("error = %v, want ErrAuthorizationRequired", err)
	}
	if called.Load() != 0 {
		t.Fatalf("handler called %d times, want 0", called.Load())
	}
}

func TestRegistryInvokesLatestVersionAndCopiesBytes(t *testing.T) {
	registry, err := NewRegistry(Options{Authorize: AllowAll})
	if err != nil {
		t.Fatalf("NewRegistry returned error: %v", err)
	}
	for version := uint32(1); version <= 2; version++ {
		version := version
		if err := registry.Register(Definition{
			Name:    "echo",
			Version: version,
			Handler: func(_ context.Context, input []byte) ([]byte, error) {
				input[0] = 'h'
				return append(input, byte('0'+version)), nil
			},
		}); err != nil {
			t.Fatalf("Register version %d returned error: %v", version, err)
		}
	}
	payload := []byte("payload")
	result, err := registry.Invoke(context.Background(), Call{Name: "echo", Payload: payload})
	if err != nil {
		t.Fatalf("Invoke returned error: %v", err)
	}
	if string(payload) != "payload" {
		t.Fatalf("caller payload mutated to %q", payload)
	}
	if string(result) != "hayload2" {
		t.Fatalf("result = %q, want handler output", result)
	}
	result[0] = 'x'
	second, err := registry.Invoke(context.Background(), Call{Name: "echo", Version: 1, Payload: []byte("payload")})
	if err != nil {
		t.Fatalf("versioned Invoke returned error: %v", err)
	}
	if string(second) != "hayload1" {
		t.Fatalf("version 1 result = %q", second)
	}
}

func TestRegistryRejectsDuplicateAndInvalidDefinitions(t *testing.T) {
	registry, err := NewRegistry(Options{Authorize: AllowAll})
	if err != nil {
		t.Fatalf("NewRegistry returned error: %v", err)
	}
	definition := Definition{Name: "echo", Version: 1, Handler: func(context.Context, []byte) ([]byte, error) { return nil, nil }}
	if err := registry.Register(definition); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := registry.Register(definition); !errors.Is(err, ErrAlreadyRegistered) {
		t.Fatalf("duplicate error = %v, want ErrAlreadyRegistered", err)
	}
	for _, invalid := range []Definition{
		{Name: "", Version: 2, Handler: definition.Handler},
		{Name: "bad name", Version: 2, Handler: definition.Handler},
		{Name: "echo", Version: 0, Handler: definition.Handler},
		{Name: "other", Version: 1},
	} {
		if err := registry.Register(invalid); !errors.Is(err, ErrInvalidDefinition) {
			t.Fatalf("definition %+v error = %v, want ErrInvalidDefinition", invalid, err)
		}
	}
}

func TestRegistryBoundsVersionsAndPanic(t *testing.T) {
	registry, err := NewRegistry(Options{Authorize: AllowAll, MaxVersionsPerProcedure: 1, MaxPayloadBytes: 4})
	if err != nil {
		t.Fatalf("NewRegistry returned error: %v", err)
	}
	if err := registry.Register(Definition{Name: "panic", Version: 1, Handler: func(context.Context, []byte) ([]byte, error) { panic("secret") }}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if err := registry.Register(Definition{Name: "panic", Version: 2, Handler: func(context.Context, []byte) ([]byte, error) { return nil, nil }}); !errors.Is(err, ErrVersionLimit) {
		t.Fatalf("version limit error = %v, want ErrVersionLimit", err)
	}
	if _, err := registry.Invoke(context.Background(), Call{Name: "panic", Payload: []byte("12345")}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("payload error = %v, want ErrPayloadTooLarge", err)
	}
	if _, err := registry.Invoke(context.Background(), Call{Name: "panic", Payload: []byte("1234")}); !errors.Is(err, ErrProcedurePanic) {
		t.Fatalf("panic error = %v, want ErrProcedurePanic", err)
	}
}

func TestRegistryListsAndRevokesDeterministically(t *testing.T) {
	registry, err := NewRegistry(Options{Authorize: AllowAll})
	if err != nil {
		t.Fatalf("NewRegistry returned error: %v", err)
	}
	for _, definition := range []Definition{
		{Name: "zeta", Version: 2, Handler: func(context.Context, []byte) ([]byte, error) { return nil, nil }},
		{Name: "alpha", Version: 3, Handler: func(context.Context, []byte) ([]byte, error) { return nil, nil }},
		{Name: "zeta", Version: 1, Handler: func(context.Context, []byte) ([]byte, error) { return nil, nil }},
	} {
		if err := registry.Register(definition); err != nil {
			t.Fatalf("Register %+v returned error: %v", definition, err)
		}
	}
	listing := registry.List()
	if len(listing) != 2 || listing[0].Name != "alpha" || listing[1].Name != "zeta" {
		t.Fatalf("listing = %+v, want sorted names", listing)
	}
	if len(listing[1].Versions) != 2 || listing[1].Versions[0] != 1 || listing[1].Versions[1] != 2 {
		t.Fatalf("zeta versions = %v, want [1 2]", listing[1].Versions)
	}
	if err := registry.Unregister("zeta", 1); err != nil {
		t.Fatalf("Unregister version returned error: %v", err)
	}
	if err := registry.Unregister("zeta", 0); err != nil {
		t.Fatalf("Unregister all returned error: %v", err)
	}
	if err := registry.Unregister("zeta", 0); !errors.Is(err, ErrProcedureNotFound) {
		t.Fatalf("second Unregister all error = %v, want ErrProcedureNotFound", err)
	}
}
