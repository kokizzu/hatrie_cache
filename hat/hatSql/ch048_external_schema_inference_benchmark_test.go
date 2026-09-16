package hatSql

import (
	"encoding/json"
	"testing"
)

var ch048ExternalSchemaBenchmarkSink []SQLRowBinaryColumn

func BenchmarkCH048ExternalSchemaInference(b *testing.B) {
	rows := make([]Row, 256)
	for index := range rows {
		rows[index] = Row{
			"active":   index%2 == 0,
			"id":       int64(index),
			"metadata": map[string]interface{}{"region": "sg", "tier": index % 3},
			"name":     "customer",
			"score":    float64(index) + 0.5,
		}
	}
	data, err := json.Marshal(rows)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("existing-json-parse", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			parsed, err := ParseJSONRows(data)
			if err != nil {
				b.Fatal(err)
			}
			if len(parsed) != len(rows) {
				b.Fatalf("parsed rows = %d, want %d", len(parsed), len(rows))
			}
		}
	})
	b.Run("json-parse-and-infer", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			columns, err := InferExternalJSONSchema(data, ExternalSchemaInferenceOptions{})
			if err != nil {
				b.Fatal(err)
			}
			ch048ExternalSchemaBenchmarkSink = columns
		}
	})
	b.Run("rows-infer", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			columns, err := InferExternalSchema(rows, ExternalSchemaInferenceOptions{})
			if err != nil {
				b.Fatal(err)
			}
			ch048ExternalSchemaBenchmarkSink = columns
		}
	})
}
