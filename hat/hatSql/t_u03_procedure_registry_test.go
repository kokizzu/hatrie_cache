package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTU03SQLProcedureRegistryCompilesAndReusesReadOnlyQueries(t *testing.T) {
	registry, err := NewSQLProcedureRegistry(SQLProcedureRegistryOptions{MaxProcedures: 4})
	if err != nil {
		t.Fatalf("NewSQLProcedureRegistry() error = %v", err)
	}
	definition := SQLProcedureDefinition{
		Name:       "user_by_id",
		Source:     "FROM VALUES (1, 'Ada'), (2, 'Lin') AS users(id, name) WHERE users.id = $1 SELECT users.name",
		Parameters: []string{"id"},
	}
	if err := registry.Register(definition); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	result, err := registry.Call(context.Background(), "user_by_id", nil, []interface{}{int64(2)}, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if got, want := result.Rows, []SQLRow{{"name": "Lin"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Call() rows = %#v, want %#v", got, want)
	}
	gotDefinition, ok := registry.Definition("USER_BY_ID")
	if !ok || !reflect.DeepEqual(gotDefinition, definition) {
		t.Fatalf("Definition() = %#v/%v, want %#v/true", gotDefinition, ok, definition)
	}
}

func TestTU03SQLProcedureRegistryRejectsDuplicatesAndUnknownCalls(t *testing.T) {
	registry, err := NewSQLProcedureRegistry(SQLProcedureRegistryOptions{MaxProcedures: 1})
	if err != nil {
		t.Fatalf("NewSQLProcedureRegistry() error = %v", err)
	}
	definition := SQLProcedureDefinition{Name: "one", Source: "FROM VALUES (1) AS one(value) SELECT one.value"}
	if err := registry.Register(definition); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := registry.Register(definition); !errors.Is(err, ErrSQLProcedureExists) {
		t.Fatalf("duplicate Register() error = %v, want ErrSQLProcedureExists", err)
	}
	if err := registry.Register(SQLProcedureDefinition{Name: "two", Source: "FROM VALUES (2) AS two(value) SELECT two.value"}); !errors.Is(err, ErrSQLProcedureLimit) {
		t.Fatalf("limit Register() error = %v, want ErrSQLProcedureLimit", err)
	}
	if _, err := registry.Call(context.Background(), "missing", nil, nil, SQLQueryOptions{}); !errors.Is(err, ErrSQLProcedureNotFound) {
		t.Fatalf("unknown Call() error = %v, want ErrSQLProcedureNotFound", err)
	}
	if _, err := registry.Call(context.Background(), "one", nil, []interface{}{int64(1)}, SQLQueryOptions{}); !errors.Is(err, ErrSQLProcedureParameterCount) {
		t.Fatalf("parameter count error = %v, want ErrSQLProcedureParameterCount", err)
	}
}

func TestTU03SQLProcedureRegistryRejectsMutationSources(t *testing.T) {
	registry, err := NewSQLProcedureRegistry(SQLProcedureRegistryOptions{})
	if err != nil {
		t.Fatalf("NewSQLProcedureRegistry() error = %v", err)
	}
	if err := registry.Register(SQLProcedureDefinition{Name: "mutating", Source: "CALL SETSTR(key => 'x', value => 'y')"}); !errors.Is(err, ErrSQLProcedureReadOnly) {
		t.Fatalf("mutating Register() error = %v, want ErrSQLProcedureReadOnly", err)
	}
	if err := registry.Register(SQLProcedureDefinition{Name: "missing_metadata", Source: "FROM VALUES ($1) AS value(row) SELECT value.row"}); !errors.Is(err, ErrSQLProcedureParameterCount) {
		t.Fatalf("parameter metadata Register() error = %v, want ErrSQLProcedureParameterCount", err)
	}
}

func TestTU03SQLProcedureRegistryBoundsAndClonesMetadata(t *testing.T) {
	if _, err := NewSQLProcedureRegistry(SQLProcedureRegistryOptions{MaxProcedures: -1}); !errors.Is(err, ErrSQLProcedureLimit) {
		t.Fatalf("negative limit error = %v, want ErrSQLProcedureLimit", err)
	}
	if _, err := NewSQLProcedureRegistry(SQLProcedureRegistryOptions{MaxProcedures: MaxSQLProcedureRegistryLimit + 1}); !errors.Is(err, ErrSQLProcedureLimit) {
		t.Fatalf("oversized limit error = %v, want ErrSQLProcedureLimit", err)
	}
	registry, err := NewSQLProcedureRegistry(SQLProcedureRegistryOptions{MaxProcedures: 2})
	if err != nil {
		t.Fatalf("NewSQLProcedureRegistry() error = %v", err)
	}
	parameters := []string{"id"}
	definition := SQLProcedureDefinition{
		Name:       "two",
		Source:     "\n\tFROM VALUES ($1) AS values(id) SELECT values.id",
		Parameters: parameters,
	}
	if err := registry.Register(definition); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	parameters[0] = "changed"
	definition.Parameters[0] = "changed-again"
	got, ok := registry.Definition(" two ")
	if !ok || !reflect.DeepEqual(got.Parameters, []string{"id"}) {
		t.Fatalf("Definition() parameters = %#v/%v, want [id]/true", got.Parameters, ok)
	}
	got.Parameters[0] = "mutated"
	gotAgain, ok := registry.Definition("TWO")
	if !ok || !reflect.DeepEqual(gotAgain.Parameters, []string{"id"}) {
		t.Fatalf("Definition() clone parameters = %#v/%v, want [id]/true", gotAgain.Parameters, ok)
	}
	definitions := registry.Definitions()
	if len(definitions) != 1 || definitions[0].Name != "two" {
		t.Fatalf("Definitions() = %#v, want one sorted definition", definitions)
	}
	definitions[0].Parameters[0] = "mutated-list"
	if gotAgain, ok := registry.Definition("two"); !ok || !reflect.DeepEqual(gotAgain.Parameters, []string{"id"}) {
		t.Fatalf("Definitions() leaked mutable parameters = %#v/%v", gotAgain.Parameters, ok)
	}
}
