package hatSql

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSQLExternalDictionaryRefreshesImmutableSnapshot(t *testing.T) {
	values := map[string]interface{}{"sg": "Asia", "count": int64(1)}
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "regions",
		Load: func(context.Context) (map[string]interface{}, error) {
			return values, nil
		},
		MaxStale:     time.Hour,
		CollectStats: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	dictionary.now = func() time.Time { return now }
	if _, _, err := dictionary.Lookup("sg"); !errors.Is(err, ErrSQLExternalDictionaryNotReady) {
		t.Fatalf("Lookup() before refresh error = %v, want not-ready", err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	values["sg"] = "mutated-after-refresh"
	got, found, err := dictionary.Lookup("sg")
	if err != nil || !found || got != "Asia" {
		t.Fatalf("Lookup(sg) = %#v/%v/%v, want Asia/true/nil", got, found, err)
	}
	if got, found, err := dictionary.Lookup("missing"); err != nil || found || got != nil {
		t.Fatalf("Lookup(missing) = %#v/%v/%v, want nil/false/nil", got, found, err)
	}
	stats := dictionary.Stats()
	if stats.Name != "regions" || stats.Entries != 2 || stats.Generation != 1 || stats.Refreshes != 1 || stats.Lookups != 3 || stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("dictionary stats = %#v", stats)
	}
}

func TestSQLExternalDictionaryRetainsValuesDuringRefreshFailureThenExpires(t *testing.T) {
	shouldFail := false
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "regions",
		Load: func(context.Context) (map[string]interface{}, error) {
			if shouldFail {
				return nil, errors.New("source unavailable")
			}
			return map[string]interface{}{"sg": "Asia"}, nil
		},
		MaxStale:     5 * time.Minute,
		CollectStats: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(200, 0)
	dictionary.now = func() time.Time { return now }
	if err := dictionary.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	shouldFail = true
	if err := dictionary.Refresh(context.Background()); err == nil {
		t.Fatal("Refresh() after loader failure returned nil")
	}
	now = now.Add(4 * time.Minute)
	if got, found, err := dictionary.Lookup("sg"); err != nil || !found || got != "Asia" {
		t.Fatalf("stale Lookup(sg) = %#v/%v/%v, want Asia/true/nil", got, found, err)
	}
	now = now.Add(2 * time.Minute)
	if _, _, err := dictionary.Lookup("sg"); !errors.Is(err, ErrSQLExternalDictionaryExpired) {
		t.Fatalf("expired Lookup(sg) error = %v, want expired", err)
	}
	stats := dictionary.Stats()
	if stats.RefreshFailures != 1 || stats.StaleHits != 1 || stats.Expired != 1 || stats.LastRefreshError != "source unavailable" {
		t.Fatalf("failure stats = %#v", stats)
	}
}

func TestSQLExternalDictionaryRegistryEvaluatesSQLFunctions(t *testing.T) {
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "regions",
		Load: func(context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"sg": "Asia"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	registry := NewSQLExternalDictionaryRegistry()
	if err := registry.Register(dictionary); err != nil {
		t.Fatal(err)
	}
	type resolver struct {
		SQLSourceResolver
		SQLFunctionResolver
	}
	source := resolver{
		SQLSourceResolver: SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
			return []SQLRow{{"region": "sg"}, {"region": "xx"}}, nil
		}),
		SQLFunctionResolver: registry,
	}
	query := "SELECT DICT_GET('regions', region) AS label, DICT_HAS('regions', region) AS known, DICT_GET_OR_DEFAULT('regions', region, 'unknown') AS fallback FROM CACHE('items')"
	result, err := ExecuteSQLQuery(query, source)
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{
		{"label": "Asia", "known": true, "fallback": "Asia"},
		{"label": nil, "known": false, "fallback": "unknown"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("dictionary SQL rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLExternalDictionaryFunctionResolverChainFallsBack(t *testing.T) {
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "regions",
		Load: func(context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"sg": "Asia"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	registry := NewSQLExternalDictionaryRegistry()
	if err := registry.Register(dictionary); err != nil {
		t.Fatal(err)
	}
	chain := NewSQLFunctionResolverChain(SQLFunctionResolverFunc(func(name string, calls []SQLFunctionCall) ([]interface{}, error) {
		if name != "custom" {
			return nil, ErrSQLFunctionNotHandled
		}
		return []interface{}{len(calls)}, nil
	}), registry)
	values, err := chain.EvaluateSQLFunction("custom", []SQLFunctionCall{{}})
	if err != nil || !reflect.DeepEqual(values, []interface{}{1}) {
		t.Fatalf("custom chain result = %#v/%v, want [1]/nil", values, err)
	}
	values, err = chain.EvaluateSQLFunction("DICT_GET", []SQLFunctionCall{{Arguments: []interface{}{"regions", "sg"}}})
	if err != nil || !reflect.DeepEqual(values, []interface{}{"Asia"}) {
		t.Fatalf("dictionary chain result = %#v/%v, want [Asia]/nil", values, err)
	}
	if _, err := chain.EvaluateSQLFunction("missing", []SQLFunctionCall{{}}); !errors.Is(err, ErrSQLFunctionNotHandled) {
		t.Fatalf("missing chain error = %v, want not-handled", err)
	}
}

func TestSQLExternalDictionaryValidationAndClose(t *testing.T) {
	for _, options := range []SQLExternalDictionaryOptions{
		{},
		{Name: "regions"},
		{Name: "regions", Load: func(context.Context) (map[string]interface{}, error) { return nil, nil }, RefreshInterval: -time.Second},
		{Name: "regions", Load: func(context.Context) (map[string]interface{}, error) { return nil, nil }, MaxStale: -time.Second},
	} {
		if _, err := NewSQLExternalDictionary(options); err == nil {
			t.Fatalf("NewSQLExternalDictionary(%#v) returned nil error", options)
		}
	}
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "regions",
		Load: func(context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"sg": "Asia"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dictionary.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dictionary.Lookup("sg"); !errors.Is(err, ErrSQLExternalDictionaryClosed) {
		t.Fatalf("closed Lookup() error = %v, want closed", err)
	}
	if err := dictionary.Refresh(context.Background()); !errors.Is(err, ErrSQLExternalDictionaryClosed) {
		t.Fatalf("closed Refresh() error = %v, want closed", err)
	}
}

func TestSQLExternalDictionaryStartsAndStopsBackgroundRefresh(t *testing.T) {
	var calls int
	var secondRefresh sync.Once
	second := make(chan struct{})
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name:            "regions",
		RefreshInterval: time.Millisecond,
		Load: func(context.Context) (map[string]interface{}, error) {
			calls++
			if calls == 2 {
				secondRefresh.Do(func() { close(second) })
			}
			return map[string]interface{}{"sg": "Asia"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dictionary.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("background refresher did not run")
	}
	if err := dictionary.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLExternalDictionaryRegistrySupportsZeroValue(t *testing.T) {
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "regions",
		Load: func(context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"sg": "Asia"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var registry SQLExternalDictionaryRegistry
	if err := registry.Register(dictionary); err != nil {
		t.Fatal(err)
	}
	if got, found, err := registry.Lookup("REGIONS", "sg"); !errors.Is(err, ErrSQLExternalDictionaryNotReady) || found || got != nil {
		t.Fatalf("zero registry before refresh = %#v/%v/%v, want nil/false/not-ready", got, found, err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, found, err := registry.Lookup("REGIONS", "sg"); err != nil || !found || got != "Asia" {
		t.Fatalf("zero registry lookup = %#v/%v/%v, want Asia/true/nil", got, found, err)
	}
	if !registry.Unregister("regions") {
		t.Fatal("Unregister() returned false for registered dictionary")
	}
	if _, _, err := registry.Lookup("regions", "sg"); !errors.Is(err, ErrSQLExternalDictionaryNotFound) {
		t.Fatalf("unregistered lookup error = %v, want not-found", err)
	}
}

func TestSQLExternalDictionaryLookupCountersAreOptIn(t *testing.T) {
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "regions",
		Load: func(context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"sg": "Asia"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dictionary.Lookup("sg"); err != nil {
		t.Fatal(err)
	}
	if stats := dictionary.Stats(); stats.Lookups != 0 || stats.Hits != 0 || stats.Misses != 0 {
		t.Fatalf("default lookup counters = %#v, want zero", stats)
	}

	dictionary, err = NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "regions",
		Load: func(context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"sg": "Asia"}, nil
		},
		CollectStats: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dictionary.Lookup("sg"); err != nil {
		t.Fatal(err)
	}
	if stats := dictionary.Stats(); stats.Lookups != 1 || stats.Hits != 1 {
		t.Fatalf("enabled lookup counters = %#v, want one lookup/hit", stats)
	}
}
