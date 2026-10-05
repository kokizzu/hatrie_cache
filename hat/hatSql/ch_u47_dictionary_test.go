package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestCHU47DictionaryFunctionsRefreshMissAndVersion(t *testing.T) {
	registry, err := NewSQLDictionaryRegistry(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(SQLDictionaryDefinition{
		Name:    "users",
		Version: 1,
		Lookup: func(key interface{}) (interface{}, bool, error) {
			if key == "u-1" {
				return "Alice", true, nil
			}
			return nil, false, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	values, err := registry.EvaluateSQLFunction("DICT_GET", []FunctionCall{{Arguments: []interface{}{"users", "u-1"}}})
	if err != nil || len(values) != 1 || values[0] != "Alice" {
		t.Fatalf("DICT_GET hit = %#v/%v, want Alice/nil", values, err)
	}
	values, err = registry.EvaluateSQLFunction("DICT_GET", []FunctionCall{{Arguments: []interface{}{"users", "missing", "fallback"}}})
	if err != nil || len(values) != 1 || values[0] != "fallback" {
		t.Fatalf("DICT_GET miss = %#v/%v, want fallback/nil", values, err)
	}
	values, err = registry.EvaluateSQLFunction("DICT_HAS", []FunctionCall{{Arguments: []interface{}{"users", "missing"}}})
	if err != nil || len(values) != 1 || values[0] != false {
		t.Fatalf("DICT_HAS miss = %#v/%v, want false/nil", values, err)
	}
	values, err = registry.EvaluateSQLFunction("DICT_VERSION", []FunctionCall{{Arguments: []interface{}{"users"}}})
	if err != nil || len(values) != 1 || values[0] != int64(1) {
		t.Fatalf("DICT_VERSION = %#v/%v, want 1/nil", values, err)
	}
	if err := registry.Refresh(SQLDictionaryDefinition{
		Name:    "users",
		Version: 2,
		Lookup: func(key interface{}) (interface{}, bool, error) {
			if key == "u-1" {
				return "Alicia", true, nil
			}
			return nil, false, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	values, err = registry.EvaluateSQLFunction("DICT_GET", []FunctionCall{{Arguments: []interface{}{"users", "u-1"}}})
	if err != nil || len(values) != 1 || values[0] != "Alicia" {
		t.Fatalf("refreshed DICT_GET = %#v/%v, want Alicia/nil", values, err)
	}
	if err := registry.Refresh(SQLDictionaryDefinition{Name: "users", Version: 1, Lookup: func(interface{}) (interface{}, bool, error) { return nil, false, nil }}); !errors.Is(err, ErrSQLDictionaryVersion) {
		t.Fatalf("downgrade error = %v, want ErrSQLDictionaryVersion", err)
	}
}

func TestCHU47DictionaryRegistryValidationAndSnapshot(t *testing.T) {
	registry, err := NewSQLDictionaryRegistry(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(SQLDictionaryDefinition{Name: "", Version: 1, Lookup: func(interface{}) (interface{}, bool, error) { return nil, false, nil }}); !errors.Is(err, ErrSQLDictionaryInvalid) {
		t.Fatalf("empty name error = %v, want ErrSQLDictionaryInvalid", err)
	}
	if err := registry.Register(SQLDictionaryDefinition{Name: "first", Version: 0, Lookup: func(interface{}) (interface{}, bool, error) { return nil, false, nil }}); !errors.Is(err, ErrSQLDictionaryVersion) {
		t.Fatalf("zero version error = %v, want ErrSQLDictionaryVersion", err)
	}
	if err := registry.Register(SQLDictionaryDefinition{Name: "first", Version: 1}); !errors.Is(err, ErrSQLDictionaryInvalid) {
		t.Fatalf("nil lookup error = %v, want ErrSQLDictionaryInvalid", err)
	}
	lookup := func(interface{}) (interface{}, bool, error) { return "value", true, nil }
	if err := registry.Register(SQLDictionaryDefinition{Name: "First", Version: 1, Lookup: lookup}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(SQLDictionaryDefinition{Name: "first", Version: 2, Lookup: lookup}); !errors.Is(err, ErrSQLDictionaryAlreadyExists) {
		t.Fatalf("duplicate error = %v, want ErrSQLDictionaryAlreadyExists", err)
	}
	if err := registry.Register(SQLDictionaryDefinition{Name: "second", Version: 1, Lookup: lookup}); !errors.Is(err, ErrSQLDictionaryInvalid) {
		t.Fatalf("capacity error = %v, want ErrSQLDictionaryInvalid", err)
	}
	snapshot := registry.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Name != "first" || snapshot[0].Version != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
	if err := registry.Remove("missing"); !errors.Is(err, ErrSQLDictionaryNotFound) {
		t.Fatalf("missing remove error = %v, want ErrSQLDictionaryNotFound", err)
	}
	if err := registry.Remove("FIRST"); err != nil {
		t.Fatal(err)
	}
}

func TestCHU47DictionaryRegistryConcurrentRefreshAndLookup(t *testing.T) {
	registry, err := NewSQLDictionaryRegistry(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(SQLDictionaryDefinition{Name: "values", Version: 1, Lookup: func(interface{}) (interface{}, bool, error) { return "v1", true, nil }}); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 100; iteration++ {
				values, err := registry.EvaluateSQLFunction("DICT_GET", []FunctionCall{{Arguments: []interface{}{"values", "key"}}})
				if err != nil || len(values) != 1 || values[0] == nil {
					t.Errorf("concurrent lookup = %#v/%v", values, err)
					return
				}
			}
		}()
	}
	for version := uint64(2); version <= 10; version++ {
		version := version
		if err := registry.Refresh(SQLDictionaryDefinition{Name: "values", Version: version, Lookup: func(interface{}) (interface{}, bool, error) { return fmt.Sprintf("v%d", version), true, nil }}); err != nil {
			t.Fatal(err)
		}
	}
	group.Wait()
}

type chu47QueryResolver struct {
	rows     []Row
	registry *SQLDictionaryRegistry
}

func (resolver chu47QueryResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver chu47QueryResolver) EvaluateSQLFunction(name string, calls []FunctionCall) ([]interface{}, error) {
	return resolver.registry.EvaluateSQLFunction(name, calls)
}

func TestCHU47DictionaryFunctionsExecuteThroughSQL(t *testing.T) {
	registry, err := NewSQLDictionaryRegistry(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(SQLDictionaryDefinition{
		Name:    "countries",
		Version: 1,
		Lookup: func(key interface{}) (interface{}, bool, error) {
			values := map[string]string{"sg": "Singapore", "id": "Indonesia"}
			value, found := values[key.(string)]
			return value, found, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	resolver := chu47QueryResolver{
		rows:     []Row{{"code": "sg"}, {"code": "xx"}},
		registry: registry,
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT DICT_GET('countries', code, 'Unknown') AS country FROM CACHE('items')", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["country"] != "Singapore" || result.Rows[1]["country"] != "Unknown" {
		t.Fatalf("dictionary SQL result = %#v", result.Rows)
	}
}

func TestCHU47DictionaryFunctionsRejectInvalidCallsAndPropagateLookupErrors(t *testing.T) {
	registry, err := NewSQLDictionaryRegistry(2)
	if err != nil {
		t.Fatal(err)
	}
	lookupErr := errors.New("upstream unavailable")
	if err := registry.Register(SQLDictionaryDefinition{Name: "broken", Version: 1, Lookup: func(interface{}) (interface{}, bool, error) { return nil, false, lookupErr }}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		args []interface{}
		want error
	}{
		{name: "DICT_GET", args: []interface{}{"missing", "key"}, want: ErrSQLDictionaryNotFound},
		{name: "DICT_GET", args: []interface{}{"broken", "key"}, want: lookupErr},
		{name: "DICT_GET", args: []interface{}{int64(1), "key"}, want: ErrSQLDictionaryInvalid},
		{name: "DICT_HAS", args: []interface{}{"broken"}, want: ErrSQLDictionaryInvalid},
		{name: "DICT_VERSION", args: []interface{}{"broken", "extra"}, want: ErrSQLDictionaryInvalid},
	} {
		if _, err := registry.EvaluateSQLFunction(test.name, []FunctionCall{{Arguments: test.args}}); !errors.Is(err, test.want) {
			t.Errorf("%s(%#v) error = %v, want %v", test.name, test.args, err, test.want)
		}
	}
	if _, err := registry.EvaluateSQLFunction("DICT_UNKNOWN", []FunctionCall{{}}); err == nil {
		t.Fatal("unknown dictionary function unexpectedly succeeded")
	}
}
