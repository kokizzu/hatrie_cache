package hatSql

import (
	"reflect"
	"strings"
	"testing"
)

type sqlDictionaryTestSource struct {
	rows []SQLRow
	*SQLDictionaryRegistry
}

func (source sqlDictionaryTestSource) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return source.rows, nil
}

func TestSQLDictionaryFunctionsProvideRefreshAndMissSemantics(t *testing.T) {
	registry := NewSQLDictionaryRegistry()
	values := map[string]interface{}{
		"sg": "Singapore",
		"id": int64(65),
	}
	if err := registry.Register("regions", values); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := registry.Register("countries", map[string]interface{}{"sg": "Singapore"}); err != nil {
		t.Fatalf("Register(countries) error = %v", err)
	}
	values["sg"] = "mutated outside registry"

	got, err := registry.EvaluateSQLFunction("DICT_GET", []FunctionCall{
		{Arguments: []interface{}{"regions", "sg"}},
		{Arguments: []interface{}{"regions", "missing", "unknown"}},
		{Arguments: []interface{}{"regions", "missing"}},
	})
	if err != nil {
		t.Fatalf("DICT_GET error = %v", err)
	}
	if want := []interface{}{"Singapore", "unknown", nil}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DICT_GET = %#v, want %#v", got, want)
	}

	has, err := registry.EvaluateSQLFunction("DICT_HAS", []FunctionCall{
		{Arguments: []interface{}{"regions", "sg"}},
		{Arguments: []interface{}{"regions", "missing"}},
	})
	if err != nil {
		t.Fatalf("DICT_HAS error = %v", err)
	}
	if want := []interface{}{true, false}; !reflect.DeepEqual(has, want) {
		t.Fatalf("DICT_HAS = %#v, want %#v", has, want)
	}

	version, err := registry.EvaluateSQLFunction("DICT_VERSION", []FunctionCall{{Arguments: []interface{}{"regions"}}})
	if err != nil || !reflect.DeepEqual(version, []interface{}{uint64(1)}) {
		t.Fatalf("DICT_VERSION = %#v/%v, want [1]/nil", version, err)
	}

	if err := registry.Refresh("regions", map[string]interface{}{"jp": "Japan"}); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	got, err = registry.EvaluateSQLFunction("DICT_GET", []FunctionCall{
		{Arguments: []interface{}{"regions", "sg", "unknown"}},
		{Arguments: []interface{}{"regions", "jp"}},
	})
	if err != nil {
		t.Fatalf("DICT_GET after refresh error = %v", err)
	}
	if want := []interface{}{"unknown", "Japan"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DICT_GET after refresh = %#v, want %#v", got, want)
	}
	version, err = registry.EvaluateSQLFunction("DICT_VERSION", []FunctionCall{{Arguments: []interface{}{"regions"}}})
	if err != nil || !reflect.DeepEqual(version, []interface{}{uint64(2)}) {
		t.Fatalf("DICT_VERSION after refresh = %#v/%v, want [2]/nil", version, err)
	}
	got, err = registry.EvaluateSQLFunction("DICT_GET", []FunctionCall{
		{Arguments: []interface{}{"regions", "jp"}},
		{Arguments: []interface{}{"countries", "sg"}},
		{Arguments: []interface{}{"REGIONS", "missing", "unknown"}},
	})
	if err != nil {
		t.Fatalf("DICT_GET mixed dictionaries error = %v", err)
	}
	if want := []interface{}{"Japan", "Singapore", "unknown"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DICT_GET mixed dictionaries = %#v, want %#v", got, want)
	}
}

func TestSQLDictionaryFunctionsIntegrateWithSQLQuery(t *testing.T) {
	registry := NewSQLDictionaryRegistry()
	if err := registry.Register("regions", map[string]interface{}{"east": "Singapore", "west": "Tokyo"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	resolver := sqlDictionaryTestSource{
		rows:                  []SQLRow{{"code": "west"}, {"code": "missing"}, {"code": "east"}},
		SQLDictionaryRegistry: registry,
	}
	result, err := ExecuteSQLQuery(`
		SELECT code, DICT_GET('regions', code, 'unknown') AS region,
			DICT_HAS('regions', code) AS known
		FROM CACHE('events')
		ORDER BY code
	`, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := []SQLRow{
		{"code": "east", "region": "Singapore", "known": true},
		{"code": "missing", "region": "unknown", "known": false},
		{"code": "west", "region": "Tokyo", "known": true},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLDictionaryFunctionsValidateArgumentsAndNames(t *testing.T) {
	registry := NewSQLDictionaryRegistry()
	if err := registry.Register("regions", map[string]interface{}{"east": "Singapore"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	for _, test := range []struct {
		name string
		args []interface{}
	}{
		{name: "missing dictionary", args: []interface{}{"missing", "east"}},
		{name: "invalid dictionary name", args: []interface{}{1, "east"}},
		{name: "invalid key", args: []interface{}{"regions", struct{}{}, "unknown"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := registry.EvaluateSQLFunction("DICT_GET", []FunctionCall{{Arguments: test.args}}); err == nil {
				t.Fatal("DICT_GET succeeded, want error")
			}
		})
	}
	if err := registry.Register("regions", map[string]interface{}{}); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("duplicate Register() error = %v, want already exists", err)
	}
	if err := registry.Refresh("missing", nil); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("unknown Refresh() error = %v, want not found", err)
	}
	if _, err := registry.EvaluateSQLFunction("UNKNOWN", []FunctionCall{{}}); err == nil {
		t.Fatal("unknown function succeeded, want error")
	}
}

func TestSQLDictionaryDefensiveCopiesAndFailedRefresh(t *testing.T) {
	raw := []byte("secret")
	registry := NewSQLDictionaryRegistry()
	if err := registry.Register("values", map[string]interface{}{"payload": raw, "old": "kept"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	raw[0] = 'X'
	value, found, version, err := registry.Lookup("VALUES", "payload")
	if err != nil || !found || version != 1 || string(value.([]byte)) != "secret" {
		t.Fatalf("Lookup() = %#v/%v/%d/%v, want secret/true/1/nil", value, found, version, err)
	}
	value.([]byte)[0] = 'Y'
	value, found, _, err = registry.Lookup("values", "payload")
	if err != nil || !found || string(value.([]byte)) != "secret" {
		t.Fatalf("Lookup() after result mutation = %#v/%v/%v, want secret/true/nil", value, found, err)
	}
	err = registry.Refresh("values", map[string]interface{}{"secret-key": map[string]interface{}{"blocked": true}})
	if err == nil {
		t.Fatal("Refresh() accepted mutable value, want error")
	}
	if strings.Contains(err.Error(), "secret-key") {
		t.Fatalf("Refresh() error exposes dictionary key: %v", err)
	}
	value, found, version, err = registry.Lookup("values", "old")
	if err != nil || !found || value != "kept" || version != 1 {
		t.Fatalf("Lookup() after failed refresh = %#v/%v/%d/%v, want kept/true/1/nil", value, found, version, err)
	}
}

func BenchmarkSQLDictionaryLookup(b *testing.B) {
	values := make(map[string]interface{}, 256)
	for index := 0; index < 256; index++ {
		values["key-"+string(rune(index))] = index
	}
	registry := NewSQLDictionaryRegistry()
	if err := registry.Register("bench", values); err != nil {
		b.Fatalf("Register() error = %v", err)
	}
	calls := make([]FunctionCall, 10000)
	keys := make([]string, len(calls))
	for index := range calls {
		keys[index] = "key-" + string(rune(index%256))
		calls[index] = FunctionCall{Arguments: []interface{}{"bench", keys[index]}}
	}
	b.Run("direct-map", func(b *testing.B) {
		b.ReportAllocs()
		var sink interface{}
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for _, key := range keys {
				sink = values[key]
			}
		}
		_ = sink
	})
	b.Run("vectorized-dict-get", func(b *testing.B) {
		b.ReportAllocs()
		var sink []interface{}
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			var err error
			sink, err = registry.EvaluateSQLFunction("DICT_GET", calls)
			if err != nil {
				b.Fatal(err)
			}
		}
		_ = sink
	})
}

func BenchmarkSQLDictionaryRefresh(b *testing.B) {
	values := make(map[string]interface{}, 10000)
	for index := 0; index < 10000; index++ {
		values["key-"+string(rune(index))] = index
	}
	registry := NewSQLDictionaryRegistry()
	if err := registry.Register("bench", values); err != nil {
		b.Fatalf("Register() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := registry.Refresh("bench", values); err != nil {
			b.Fatal(err)
		}
	}
}
