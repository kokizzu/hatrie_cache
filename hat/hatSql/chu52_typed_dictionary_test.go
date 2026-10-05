package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCHU52TypedDictionaryRefreshesAndUsesTypedKeys(t *testing.T) {
	loads := 0
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name:    "metrics",
		KeyKind: SQLExternalDictionaryKeyInt64,
		LoadEntries: func(context.Context) ([]SQLExternalDictionaryEntry, error) {
			loads++
			if loads == 1 {
				return []SQLExternalDictionaryEntry{{Key: int64(7), Value: "seven"}, {Key: int64(8), Value: int64(80)}}, nil
			}
			return []SQLExternalDictionaryEntry{{Key: int64(7), Value: "updated"}}, nil
		},
		CollectStats: true,
	})
	if err != nil {
		t.Fatalf("NewSQLExternalDictionary() error = %v", err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		t.Fatalf("first Refresh() error = %v", err)
	}
	if value, found, err := dictionary.LookupKey(int32(7)); err != nil || !found || value != "seven" {
		t.Fatalf("typed LookupKey() = %#v/%v/%v", value, found, err)
	}
	if value, found, err := dictionary.LookupKey(int64(8)); err != nil || !found || value != int64(80) {
		t.Fatalf("second typed LookupKey() = %#v/%v/%v", value, found, err)
	}
	if _, _, err := dictionary.LookupKey("7"); !errors.Is(err, ErrSQLExternalDictionaryKeyType) {
		t.Fatalf("wrong key type error = %v", err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		t.Fatalf("second Refresh() error = %v", err)
	}
	if _, found, err := dictionary.LookupKey(int64(8)); err != nil || found {
		t.Fatalf("replaced snapshot lookup = found %v/error %v", found, err)
	}
	if value, found, err := dictionary.LookupKey(int64(7)); err != nil || !found || value != "updated" {
		t.Fatalf("replaced key lookup = %#v/%v/%v", value, found, err)
	}
	if stats := dictionary.Stats(); stats.Generation != 2 || stats.Entries != 1 || stats.Hits != 3 || stats.Misses != 1 {
		t.Fatalf("typed dictionary stats = %#v", stats)
	}
}

func TestCHU52TypedDictionaryFunctionsAcceptNumericAndTimeKeys(t *testing.T) {
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name:    "events",
		KeyKind: SQLExternalDictionaryKeyTime,
		LoadEntries: func(context.Context) ([]SQLExternalDictionaryEntry, error) {
			return []SQLExternalDictionaryEntry{{Key: time.Unix(7, 0).UTC(), Value: "ready"}}, nil
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
	values, err := registry.EvaluateSQLFunction("DICT_GET_OR_DEFAULT", []FunctionCall{
		{Arguments: []interface{}{"events", time.Unix(7, 0).UTC(), "missing"}},
		{Arguments: []interface{}{"events", time.Unix(8, 0).UTC(), "missing"}},
	})
	if err != nil || len(values) != 2 || values[0] != "ready" || values[1] != "missing" {
		t.Fatalf("typed DICT_GET_OR_DEFAULT() = %#v/%v", values, err)
	}
	has, err := registry.EvaluateSQLFunction("DICT_HAS", []FunctionCall{{Arguments: []interface{}{"events", time.Unix(7, 0).UTC()}}})
	if err != nil || len(has) != 1 || has[0] != true {
		t.Fatalf("typed DICT_HAS() = %#v/%v", has, err)
	}
}

func TestCHU52TypedDictionaryValidationAndFailedRefreshKeepSnapshot(t *testing.T) {
	if _, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name:        "invalid",
		KeyKind:     SQLExternalDictionaryKeyInt64,
		Load:        func(context.Context) (map[string]interface{}, error) { return nil, nil },
		LoadEntries: func(context.Context) ([]SQLExternalDictionaryEntry, error) { return nil, nil },
	}); !errors.Is(err, ErrSQLExternalDictionaryLoadConflict) {
		t.Fatalf("load conflict error = %v", err)
	}
	if _, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name:    "invalid",
		KeyKind: SQLExternalDictionaryKeyKind("bad"),
		LoadEntries: func(context.Context) ([]SQLExternalDictionaryEntry, error) {
			return nil, nil
		},
	}); !errors.Is(err, ErrSQLExternalDictionaryKeyKindInvalid) {
		t.Fatalf("key kind error = %v", err)
	}
	refreshErr := errors.New("source unavailable")
	loads := 0
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name:    "resilient",
		KeyKind: SQLExternalDictionaryKeyUint64,
		LoadEntries: func(context.Context) ([]SQLExternalDictionaryEntry, error) {
			loads++
			if loads > 1 {
				return nil, refreshErr
			}
			return []SQLExternalDictionaryEntry{{Key: uint64(1), Value: "one"}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := dictionary.Refresh(context.Background()); !errors.Is(err, refreshErr) {
		t.Fatalf("failed refresh error = %v", err)
	}
	if value, found, err := dictionary.LookupKey(uint64(1)); err != nil || !found || value != "one" {
		t.Fatalf("snapshot after failed refresh = %#v/%v/%v", value, found, err)
	}
}

func TestCHU52TypedDictionarySupportsStringAndUint64Keys(t *testing.T) {
	tests := []struct {
		name      string
		keyKind   SQLExternalDictionaryKeyKind
		entries   []SQLExternalDictionaryEntry
		lookupKey interface{}
	}{
		{
			name:    "string",
			keyKind: SQLExternalDictionaryKeyString,
			entries: []SQLExternalDictionaryEntry{
				{Key: "apac", Value: "region-apac"},
			},
			lookupKey: "apac",
		},
		{
			name:    "uint64",
			keyKind: SQLExternalDictionaryKeyUint64,
			entries: []SQLExternalDictionaryEntry{
				{Key: uint64(42), Value: "answer"},
			},
			lookupKey: uint32(42),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
				Name:    test.name,
				KeyKind: test.keyKind,
				LoadEntries: func(context.Context) ([]SQLExternalDictionaryEntry, error) {
					return test.entries, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := dictionary.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			value, found, err := dictionary.LookupKey(test.lookupKey)
			if err != nil {
				t.Fatal(err)
			}
			if !found || value != test.entries[0].Value {
				t.Fatalf("lookup = (%v, %v), want (%v, true)", value, found, test.entries[0].Value)
			}
		})
	}
}
