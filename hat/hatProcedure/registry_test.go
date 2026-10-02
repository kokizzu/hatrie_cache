package hatProcedure_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"hatrie_cache/hat/hatProcedure"
)

func TestRegistryCallAuthorizesVersionsAndCopiesArguments(t *testing.T) {
	var authorized atomic.Int64
	registry, err := hatProcedure.NewRegistry(hatProcedure.RegistryOptions{
		Authorize: func(_ context.Context, principal string, info hatProcedure.ProcedureInfo) error {
			if principal != "writer" || info.Name != "echo" || info.Version != 2 {
				return errors.New("unexpected authorization request")
			}
			authorized.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	if err := registry.Register(hatProcedure.Definition{
		Name:    "echo",
		Version: 2,
		Handler: func(_ context.Context, call hatProcedure.Call) ([]byte, error) {
			call.Arguments[0] = 'X'
			return append([]byte(nil), call.Arguments...), nil
		},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	arguments := []byte("hello")
	got, err := registry.Call(context.Background(), "writer", "echo", 2, arguments)
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if string(got) != "Xello" {
		t.Fatalf("Call() = %q, want Xello", got)
	}
	if string(arguments) != "hello" {
		t.Fatalf("Call() exposed caller buffer to handler: %q", arguments)
	}
	if authorized.Load() != 1 {
		t.Fatalf("authorization calls = %d, want 1", authorized.Load())
	}
	if _, err := registry.Call(context.Background(), "writer", "echo", 1, nil); !errors.Is(err, hatProcedure.ErrProcedureNotFound) {
		t.Fatalf("wrong version error = %v, want ErrProcedureNotFound", err)
	}
}

func TestRegistryDefaultsToBoundedConfigurationAndDefaultDeny(t *testing.T) {
	if _, err := hatProcedure.NewRegistry(hatProcedure.RegistryOptions{}); !errors.Is(err, hatProcedure.ErrAuthorizerRequired) {
		t.Fatalf("NewRegistry() without authorizer error = %v, want ErrAuthorizerRequired", err)
	}
	registry, err := hatProcedure.NewRegistry(hatProcedure.RegistryOptions{
		Authorize: func(context.Context, string, hatProcedure.ProcedureInfo) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	if err := registry.Register(hatProcedure.Definition{Name: "echo", Version: 1, Handler: func(context.Context, hatProcedure.Call) ([]byte, error) { return nil, nil }}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "anonymous", "echo", 1, nil); err != nil {
		t.Fatalf("authorized default call error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "anonymous", "missing", 1, nil); !errors.Is(err, hatProcedure.ErrProcedureNotFound) {
		t.Fatalf("missing procedure error = %v, want ErrProcedureNotFound", err)
	}
}

func TestRegistryWrapsAuthorizationFailures(t *testing.T) {
	registry, err := hatProcedure.NewRegistry(hatProcedure.RegistryOptions{
		Authorize: func(context.Context, string, hatProcedure.ProcedureInfo) error {
			return errors.New("denied by policy")
		},
	})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	if err := registry.Register(hatProcedure.Definition{Name: "secret", Version: 1, Handler: func(context.Context, hatProcedure.Call) ([]byte, error) {
		return []byte("must not run"), nil
	}}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "reader", "secret", 1, nil); !errors.Is(err, hatProcedure.ErrProcedureUnauthorized) || !strings.Contains(err.Error(), "denied by policy") {
		t.Fatalf("authorization error = %v, want wrapped denial", err)
	}
}

func TestRegistryRejectsInvalidDefinitionsAndLimits(t *testing.T) {
	registry, err := hatProcedure.NewRegistry(hatProcedure.RegistryOptions{
		MaxProcedures:     1,
		MaxArgumentsBytes: 3,
		MaxResultBytes:    3,
		Authorize:         func(context.Context, string, hatProcedure.ProcedureInfo) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	cases := []struct {
		name string
		def  hatProcedure.Definition
		want error
	}{
		{"empty name", hatProcedure.Definition{Version: 1, Handler: func(context.Context, hatProcedure.Call) ([]byte, error) { return nil, nil }}, hatProcedure.ErrDefinitionInvalid},
		{"version zero", hatProcedure.Definition{Name: "zero", Handler: func(context.Context, hatProcedure.Call) ([]byte, error) { return nil, nil }}, hatProcedure.ErrDefinitionInvalid},
		{"nil handler", hatProcedure.Definition{Name: "nil", Version: 1}, hatProcedure.ErrDefinitionInvalid},
		{"space name", hatProcedure.Definition{Name: "bad name", Version: 1, Handler: func(context.Context, hatProcedure.Call) ([]byte, error) { return nil, nil }}, hatProcedure.ErrDefinitionInvalid},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if err := registry.Register(tt.def); !errors.Is(err, tt.want) {
				t.Fatalf("Register() error = %v, want %v", err, tt.want)
			}
		})
	}
	definition := hatProcedure.Definition{Name: "limited", Version: 1, Handler: func(_ context.Context, call hatProcedure.Call) ([]byte, error) {
		return []byte("four"), nil
	}}
	if err := registry.Register(definition); err != nil {
		t.Fatalf("Register(limited) error = %v", err)
	}
	if err := registry.Register(definition); !errors.Is(err, hatProcedure.ErrProcedureAlreadyRegistered) {
		t.Fatalf("duplicate Register() error = %v, want ErrProcedureAlreadyRegistered", err)
	}
	if _, err := registry.Call(context.Background(), "p", "limited", 1, []byte("1234")); !errors.Is(err, hatProcedure.ErrArgumentsTooLarge) {
		t.Fatalf("large arguments error = %v, want ErrArgumentsTooLarge", err)
	}
	if _, err := registry.Call(context.Background(), "p", "limited", 1, nil); !errors.Is(err, hatProcedure.ErrResultTooLarge) {
		t.Fatalf("large result error = %v, want ErrResultTooLarge", err)
	}
	if err := registry.Register(hatProcedure.Definition{Name: "second", Version: 1, Handler: definition.Handler}); !errors.Is(err, hatProcedure.ErrProcedureLimit) {
		t.Fatalf("procedure limit error = %v, want ErrProcedureLimit", err)
	}
}

func TestRegistryPanicCancellationAndListing(t *testing.T) {
	registry, err := hatProcedure.NewRegistry(hatProcedure.RegistryOptions{
		Authorize: func(context.Context, string, hatProcedure.ProcedureInfo) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}
	if err := registry.Register(hatProcedure.Definition{Name: "zeta", Version: 2, Handler: func(context.Context, hatProcedure.Call) ([]byte, error) { panic("boom") }}); err != nil {
		t.Fatalf("Register(zeta) error = %v", err)
	}
	if err := registry.Register(hatProcedure.Definition{Name: "alpha", Version: 1, Handler: func(ctx context.Context, _ hatProcedure.Call) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }}); err != nil {
		t.Fatalf("Register(alpha) error = %v", err)
	}
	if _, err := registry.Call(context.Background(), "p", "zeta", 2, nil); !errors.Is(err, hatProcedure.ErrProcedurePanic) {
		t.Fatalf("panic call error = %v, want ErrProcedurePanic", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Call(ctx, "p", "alpha", 1, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call error = %v, want context.Canceled", err)
	}
	list := registry.List()
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	if len(list) != 2 || list[0].Name != "alpha" || list[1].Name != "zeta" {
		t.Fatalf("List() = %#v, want alpha/zeta", list)
	}
	if !registry.Unregister("alpha", 1) || registry.Unregister("missing", 1) {
		t.Fatal("Unregister() result mismatch")
	}
	if got := registry.List(); len(got) != 1 || got[0].Name != "zeta" {
		t.Fatalf("List() after unregister = %#v, want zeta", got)
	}
	if _, err := registry.Call(context.Background(), "p", "zeta", 2, nil); !strings.Contains(err.Error(), "procedure panic") {
		t.Fatalf("panic error = %v, want diagnostic", err)
	}
}
