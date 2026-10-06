package hatSql_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestTU03StoredProcedureRegistry(t *testing.T) {
	var authorizations []hatSql.StoredProcedureAuthorization
	registry, err := hatSql.NewStoredProcedureRegistry(hatSql.StoredProcedureRegistryOptions{
		MaxProcedures:      8,
		MaxArguments:       3,
		MaxInputBytes:      64,
		MaxOutputBytes:     64,
		MaxConcurrentCalls: 2,
		MaxCallDuration:    time.Second,
		Authorize: func(_ context.Context, authorization hatSql.StoredProcedureAuthorization) error {
			authorizations = append(authorizations, authorization)
			if authorization.Principal == "denied" {
				return hatSql.ErrStoredProcedureUnauthorized
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := registry.Register(hatSql.StoredProcedureDefinition{
		Package: "app",
		Name:    "sum",
		Version: "v1",
		Evaluate: func(_ context.Context, arguments []interface{}) (interface{}, error) {
			return arguments[0].(int64) + arguments[1].(int64), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(hatSql.StoredProcedureDefinition{
		Package:  "app",
		Name:     "sum",
		Version:  "v1",
		Evaluate: func(context.Context, []interface{}) (interface{}, error) { return nil, nil },
	}); !errors.Is(err, hatSql.ErrStoredProcedureDuplicate) {
		t.Fatalf("duplicate registration error = %v, want duplicate", err)
	}

	value, err := registry.Call(context.Background(), "alice", "APP", "SUM", "v1", []interface{}{int64(2), int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	if value != int64(5) {
		t.Fatalf("Call() = %#v, want 5", value)
	}
	if !reflect.DeepEqual(authorizations, []hatSql.StoredProcedureAuthorization{{Principal: "alice", Package: "app", Name: "sum", Version: "v1"}}) {
		t.Fatalf("authorizations = %#v", authorizations)
	}

	definition, ok := registry.Definition("app", "sum", "v1")
	if !ok || definition.Name != "sum" || definition.Version != "v1" {
		t.Fatalf("Definition() = %#v, %v", definition, ok)
	}
	definitions := registry.Definitions()
	if len(definitions) != 1 {
		t.Fatalf("Definitions() length = %d, want 1", len(definitions))
	}

	if _, err := registry.Call(context.Background(), "denied", "app", "sum", "v1", nil); !errors.Is(err, hatSql.ErrStoredProcedureUnauthorized) {
		t.Fatalf("denied Call() error = %v, want unauthorized", err)
	}
	if _, err := registry.Call(context.Background(), "alice", "app", "missing", "v1", nil); !errors.Is(err, hatSql.ErrStoredProcedureNotFound) {
		t.Fatalf("missing Call() error = %v, want not found", err)
	}
	if _, err := registry.Call(nil, "alice", "app", "sum", "v1", nil); !errors.Is(err, hatSql.ErrStoredProcedureContextRequired) {
		t.Fatalf("nil context error = %v, want context required", err)
	}
	if _, err := registry.Call(context.Background(), "alice", "app", "sum", "v1", []interface{}{int64(1), int64(2), int64(3), int64(4)}); !errors.Is(err, hatSql.ErrStoredProcedureArgumentLimit) {
		t.Fatalf("argument limit error = %v, want argument limit", err)
	}
	if _, err := registry.Call(context.Background(), "alice", "app", "sum", "v1", []interface{}{strings.Repeat("x", 65)}); !errors.Is(err, hatSql.ErrStoredProcedureInputLimit) {
		t.Fatalf("input limit error = %v, want input limit", err)
	}

	if err := registry.Register(hatSql.StoredProcedureDefinition{
		Package: "app",
		Name:    "mutate",
		Version: "v1",
		Evaluate: func(_ context.Context, arguments []interface{}) (interface{}, error) {
			arguments[0].([]byte)[0] = 'X'
			return arguments[0], nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	original := []byte("value")
	if _, err := registry.Call(context.Background(), "alice", "app", "mutate", "v1", []interface{}{original}); err != nil {
		t.Fatal(err)
	}
	if string(original) != "value" {
		t.Fatalf("caller input mutated to %q", original)
	}

	if err := registry.Register(hatSql.StoredProcedureDefinition{
		Package:  "app",
		Name:     "panic",
		Version:  "v1",
		Evaluate: func(context.Context, []interface{}) (interface{}, error) { panic("procedure panic") },
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Call(context.Background(), "alice", "app", "panic", "v1", nil); !errors.Is(err, hatSql.ErrStoredProcedurePanic) {
		t.Fatalf("panic Call() error = %v, want panic error", err)
	}

	if err := registry.Register(hatSql.StoredProcedureDefinition{
		Package: "app",
		Name:    "large",
		Version: "v1",
		Evaluate: func(context.Context, []interface{}) (interface{}, error) {
			return strings.Repeat("x", 65), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Call(context.Background(), "alice", "app", "large", "v1", nil); !errors.Is(err, hatSql.ErrStoredProcedureOutputLimit) {
		t.Fatalf("output limit error = %v, want output limit", err)
	}

	if err := registry.Register(hatSql.StoredProcedureDefinition{Package: "app", Name: "invalid", Version: "v1"}); !errors.Is(err, hatSql.ErrStoredProcedureDefinitionInvalid) {
		t.Fatalf("invalid definition error = %v, want invalid definition", err)
	}
	if err := registry.Register(hatSql.StoredProcedureDefinition{Package: "app", Name: "bad\x00name", Version: "v1", Evaluate: func(context.Context, []interface{}) (interface{}, error) { return nil, nil }}); !errors.Is(err, hatSql.ErrStoredProcedureDefinitionInvalid) {
		t.Fatalf("control-character definition error = %v, want invalid definition", err)
	}
}
