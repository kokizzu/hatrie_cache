package hatDictionary_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDictionary"
	"hatrie_cache/hat/hatSql"
)

func TestCH029DictionarySQLLookupResolverAvoidsDimensionScan(t *testing.T) {
	var sourceCalls int
	dictionary, err := hatDictionary.New(hatDictionary.SourceFunc(func(_ context.Context, keys []string) (map[string]string, error) {
		sourceCalls++
		values := map[string]string{}
		for _, key := range keys {
			switch key {
			case "SG":
				values[key] = "Singapore"
			case "JP":
				values[key] = "Japan"
			}
		}
		return values, nil
	}), hatDictionary.Options{})
	if err != nil {
		t.Fatal(err)
	}
	base := hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		if name == "CACHE" && key == "orders" {
			return []hatSql.Row{
				{"id": int64(1), "country": "SG"},
				{"id": int64(2), "country": "JP"},
				{"id": int64(3), "country": "XX"},
			}, nil
		}
		return nil, nil
	})
	resolver, err := hatDictionary.NewSQLDictionaryLookupResolver(base, dictionary, hatDictionary.SQLDictionaryLookupOptions{
		SourceKey:  "countries",
		KeyField:   "code",
		ValueField: "name",
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM CACHE('orders') AS order_row
JOIN EXTERNAL('countries') AS country ON order_row.country = country.code
SELECT order_row.id, country.name
ORDER BY order_row.id`, resolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{
		{"id": int64(1), "name": "Singapore"},
		{"id": int64(2), "name": "Japan"},
	}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("inner join rows = %#v, want %#v", result.Rows, want)
	}
	if sourceCalls != 3 {
		t.Fatalf("dictionary source calls = %d, want one refresh per distinct order key", sourceCalls)
	}
}

func TestCH029DictionarySQLLookupResolverPreservesLeftMissesAndDelegatesBase(t *testing.T) {
	dictionary, err := hatDictionary.New(hatDictionary.SourceFunc(func(_ context.Context, keys []string) (map[string]string, error) {
		return map[string]string{"SG": "Singapore"}, nil
	}), hatDictionary.Options{})
	if err != nil {
		t.Fatal(err)
	}
	baseCalls := 0
	base := hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		baseCalls++
		if name == "CACHE" && key == "orders" {
			return []hatSql.Row{{"id": int64(1), "country": "XX"}}, nil
		}
		return nil, nil
	})
	resolver, err := hatDictionary.NewSQLDictionaryLookupResolver(base, dictionary, hatDictionary.SQLDictionaryLookupOptions{
		SourceKey:  "countries",
		KeyField:   "code",
		ValueField: "name",
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM CACHE('orders') AS order_row
LEFT JOIN EXTERNAL('countries') AS country ON order_row.country = country.code
SELECT order_row.id, country.name`, resolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": int64(1), "name": nil}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("left join rows = %#v, want %#v", result.Rows, want)
	}
	if baseCalls != 1 {
		t.Fatalf("base resolver calls = %d, want one", baseCalls)
	}
}

func TestCH029DictionarySQLLookupResolverSupportsTypedKeys(t *testing.T) {
	dictionary, err := hatDictionary.New(hatDictionary.SourceFunc(func(_ context.Context, keys []string) (map[string]string, error) {
		return map[string]string{"42": "answer"}, nil
	}), hatDictionary.Options{})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := hatDictionary.NewSQLDictionaryLookupResolver(
		hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
			return []hatSql.Row{{"id": int64(42)}}, nil
		}),
		dictionary,
		hatDictionary.SQLDictionaryLookupOptions{SourceKey: "numbers", KeyField: "id", ValueField: "label"},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM CACHE('items') AS item
JOIN EXTERNAL('numbers') AS number ON item.id = number.id
SELECT number.label`, resolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"label": "answer"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("typed-key rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCH029DictionarySQLLookupResolverPinsExpectedVersion(t *testing.T) {
	version := "v1"
	dictionary, err := hatDictionary.New(hatDictionary.VersionedSourceFunc(func(_ context.Context, keys []string) (map[string]string, string, error) {
		values := make(map[string]string, len(keys))
		for _, key := range keys {
			values[key] = "label-" + version
		}
		return values, version, nil
	}), hatDictionary.Options{})
	if err != nil {
		t.Fatal(err)
	}
	base := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return nil, nil })
	first, err := hatDictionary.NewSQLDictionaryLookupResolver(base, dictionary, hatDictionary.SQLDictionaryLookupOptions{
		SourceKey:       "versions",
		KeyField:        "id",
		ValueField:      "label",
		ExpectedVersion: "v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, available, err := first.ResolveSQLLookupSource("EXTERNAL", "versions", "id", "SG")
	if err != nil || !available || !reflect.DeepEqual(rows, []hatSql.Row{{"id": "SG", "label": "label-v1"}}) {
		t.Fatalf("v1 lookup = %#v, %v, %v", rows, available, err)
	}

	version = "v2"
	second, err := hatDictionary.NewSQLDictionaryLookupResolver(base, dictionary, hatDictionary.SQLDictionaryLookupOptions{
		SourceKey:       "versions",
		KeyField:        "id",
		ValueField:      "label",
		ExpectedVersion: "v2",
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, available, err = second.ResolveSQLLookupSource("EXTERNAL", "versions", "id", "SG")
	if err != nil || !available || !reflect.DeepEqual(rows, []hatSql.Row{{"id": "SG", "label": "label-v2"}}) {
		t.Fatalf("v2 lookup = %#v, %v, %v", rows, available, err)
	}
}

func TestCH029DictionarySQLLookupResolverValidatesOptionsAndDoesNotFakeFullScan(t *testing.T) {
	dictionary, err := hatDictionary.New(hatDictionary.SourceFunc(func(context.Context, []string) (map[string]string, error) {
		return nil, nil
	}), hatDictionary.Options{})
	if err != nil {
		t.Fatal(err)
	}
	base := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return nil, nil })
	for _, options := range []hatDictionary.SQLDictionaryLookupOptions{
		{SourceKey: "", KeyField: "id", ValueField: "label"},
		{SourceKey: "items", KeyField: "", ValueField: "label"},
		{SourceKey: "items", KeyField: "id", ValueField: "id"},
	} {
		if _, err := hatDictionary.NewSQLDictionaryLookupResolver(base, dictionary, options); !errors.Is(err, hatDictionary.ErrSQLLookupOptionsInvalid) {
			t.Fatalf("options %#v error = %v, want ErrSQLLookupOptionsInvalid", options, err)
		}
	}
	resolver, err := hatDictionary.NewSQLDictionaryLookupResolver(base, dictionary, hatDictionary.SQLDictionaryLookupOptions{
		SourceKey:  "items",
		KeyField:   "id",
		ValueField: "label",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveSQLExternalSource("items"); !errors.Is(err, hatDictionary.ErrSQLFullScanUnavailable) {
		t.Fatalf("full scan error = %v, want ErrSQLFullScanUnavailable", err)
	}
	if rows, available, err := resolver.ResolveSQLLookupSource("EXTERNAL", "other", "id", "x"); err != nil || available || rows != nil {
		t.Fatalf("unbound lookup = %#v, %v, %v; want unavailable", rows, available, err)
	}
}
