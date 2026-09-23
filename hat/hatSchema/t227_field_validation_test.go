package hatSchema

import (
	"strings"
	"testing"
	"time"
)

func TestT227ValidateRowsEnforcesDeclaredFieldTypesAndNullability(t *testing.T) {
	schema := Schema{Sources: map[string]Source{
		"events": {
			Name: "events",
			Columns: []Column{
				{Name: "id", Type: TypeInteger, NotNull: true},
				{Name: "name", Type: TypeText},
				{Name: "score", Type: TypeNumber},
				{Name: "enabled", Type: TypeBoolean},
				{Name: "state", Type: TypeEnum8, EnumValues: []string{"new", "ready"}},
				{Name: "created", Type: TypeTimestamp},
				{Name: "raw", Type: TypeBinary},
				{Name: "payload", Type: TypeJSON},
			},
		},
	}}
	valid := Row{
		"id":      int64(7),
		"name":    "sample",
		"score":   float64(1.5),
		"enabled": true,
		"state":   "ready",
		"created": time.Unix(1700000000, 0).UTC(),
		"raw":     []byte{1, 2, 3},
		"payload": map[string]interface{}{"ok": true},
	}
	if err := ValidateRows(schema, "events", []Row{valid}, nil); err != nil {
		t.Fatalf("ValidateRows(valid) error = %v", err)
	}
	nullable := make(Row, len(valid))
	for key, value := range valid {
		nullable[key] = value
	}
	nullable["name"] = nil
	if err := ValidateRows(schema, "events", []Row{nullable}, nil); err != nil {
		t.Fatalf("ValidateRows(nullable) error = %v", err)
	}

	cases := []struct {
		name  string
		field string
		value interface{}
		want  string
	}{
		{name: "integer type", field: "id", value: "7", want: "id"},
		{name: "text type", field: "name", value: 7, want: "name"},
		{name: "number type", field: "score", value: "1.5", want: "score"},
		{name: "boolean type", field: "enabled", value: "true", want: "enabled"},
		{name: "enum value", field: "state", value: "unknown", want: "state"},
		{name: "timestamp type", field: "created", value: "2023-11-14T22:13:20Z", want: "created"},
		{name: "binary type", field: "raw", value: "AQI=", want: "raw"},
		{name: "not null", field: "id", value: nil, want: "NOT NULL"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			row := make(Row, len(valid))
			for key, value := range valid {
				row[key] = value
			}
			row[test.field] = test.value
			err := ValidateRows(schema, "events", []Row{row}, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateRows(%s) error = %v, want diagnostic containing %q", test.name, err, test.want)
			}
		})
	}
}

func BenchmarkT227ValidateRows(b *testing.B) {
	schema := Schema{Sources: map[string]Source{
		"events": {
			Name: "events",
			Columns: []Column{
				{Name: "id", Type: TypeInteger, NotNull: true},
				{Name: "name", Type: TypeText},
				{Name: "enabled", Type: TypeBoolean},
			},
		},
	}}
	rows := []Row{{"id": int64(7), "name": "sample", "enabled": true}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := ValidateRows(schema, "events", rows, nil); err != nil {
			b.Fatal(err)
		}
	}
}
