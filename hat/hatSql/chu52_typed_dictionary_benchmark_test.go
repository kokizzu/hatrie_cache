package hatSql

import (
	"context"
	"testing"
)

var benchmarkCHU52TypedDictionarySink interface{}

func BenchmarkCHU52TypedDictionaryLookup(b *testing.B) {
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name:    "bench",
		KeyKind: SQLExternalDictionaryKeyInt64,
		LoadEntries: func(context.Context) ([]SQLExternalDictionaryEntry, error) {
			entries := make([]SQLExternalDictionaryEntry, 1024)
			for key := range entries {
				entries[key] = SQLExternalDictionaryEntry{Key: int64(key), Value: "label"}
			}
			return entries, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		benchmarkCHU52TypedDictionarySink, _, err = dictionary.LookupKey(int64(index) & 1023)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU52LegacyStringDictionaryLookup(b *testing.B) {
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "legacy",
		Load: func(context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"key": int64(42)}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, found, err := dictionary.Lookup("key")
		if err != nil || !found || value.(int64) != 42 {
			b.Fatalf("lookup = (%v, %v, %v)", value, found, err)
		}
	}
}
