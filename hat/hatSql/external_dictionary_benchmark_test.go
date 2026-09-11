package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCH049ExternalDictionaryLookup(b *testing.B) {
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "regions",
		Load: func(context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"sg": "Asia", "us": "America", "jp": "Japan"}, nil
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
		ch049ExternalDictionaryBenchmarkSink, _, _ = dictionary.Lookup("sg")
	}
}

func BenchmarkCH049ExternalDictionaryLookupWithStats(b *testing.B) {
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name:         "regions",
		CollectStats: true,
		Load: func(context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"sg": "Asia", "us": "America", "jp": "Japan"}, nil
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
		ch049ExternalDictionaryBenchmarkSink, _, _ = dictionary.Lookup("sg")
	}
}

func BenchmarkCH049ExternalDictionaryFunctionLookup(b *testing.B) {
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "regions",
		Load: func(context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"sg": "Asia", "us": "America", "jp": "Japan"}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	registry := NewSQLExternalDictionaryRegistry()
	if err := registry.Register(dictionary); err != nil {
		b.Fatal(err)
	}
	calls := []FunctionCall{{Arguments: []interface{}{"regions", "sg"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		values, err := registry.EvaluateSQLFunction("DICT_GET", calls)
		if err != nil {
			b.Fatal(err)
		}
		ch049ExternalDictionaryBenchmarkSink = values[0]
	}
}

func BenchmarkCH049ExternalDictionaryRefresh(b *testing.B) {
	dictionary, err := NewSQLExternalDictionary(SQLExternalDictionaryOptions{
		Name: "regions",
		Load: func(context.Context) (map[string]interface{}, error) {
			values := make(map[string]interface{}, 256)
			for index := 0; index < 256; index++ {
				values[string(rune('a'+index%26))+string(rune('0'+index/26))] = int64(index)
			}
			return values, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := dictionary.Refresh(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
