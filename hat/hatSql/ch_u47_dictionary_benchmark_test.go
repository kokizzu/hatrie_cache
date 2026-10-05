package hatSql

import "testing"

const chu47DictionaryBatchSize = 256

func BenchmarkCHU47BaselineMapLookup(b *testing.B) {
	values := map[string]string{"sg": "Singapore", "id": "Indonesia", "my": "Malaysia", "th": "Thailand"}
	keys := chu47DictionaryKeys()
	results := make([]interface{}, len(keys))
	b.ReportAllocs()
	b.ReportMetric(float64(len(keys)), "lookups/op")
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for index, key := range keys {
			results[index] = values[key]
		}
	}
	_ = results
}

func BenchmarkCHU47DictionaryLookup(b *testing.B) {
	values := map[string]string{"sg": "Singapore", "id": "Indonesia", "my": "Malaysia", "th": "Thailand"}
	registry, err := NewSQLDictionaryRegistry(1)
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register(SQLDictionaryDefinition{
		Name:    "countries",
		Version: 1,
		Lookup: func(key interface{}) (interface{}, bool, error) {
			keyString, ok := key.(string)
			if !ok {
				return nil, false, nil
			}
			value, found := values[keyString]
			return value, found, nil
		},
	}); err != nil {
		b.Fatal(err)
	}
	calls := make([]FunctionCall, chu47DictionaryBatchSize)
	keys := chu47DictionaryKeys()
	for index := range calls {
		calls[index] = FunctionCall{Arguments: []interface{}{"countries", keys[index]}}
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(calls)), "lookups/op")
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := registry.EvaluateSQLFunction("DICT_GET", calls); err != nil {
			b.Fatal(err)
		}
	}
}

func chu47DictionaryKeys() []string {
	values := []string{"sg", "id", "my", "th"}
	keys := make([]string, chu47DictionaryBatchSize)
	for index := range keys {
		keys[index] = values[index%len(values)]
	}
	return keys
}
