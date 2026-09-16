package hatSql

import (
	"reflect"
	"testing"
	"time"
)

func TestCH048ExternalSchemaInferencePromotesAndPreservesColumns(t *testing.T) {
	rows := []Row{
		{"id": int64(7), "name": "alpha", "score": int64(1), "active": true},
		{"id": int64(8), "name": nil, "score": float64(1.5)},
	}
	columns, err := InferExternalSchema(rows, ExternalSchemaInferenceOptions{AllowNumericPromotion: true})
	if err != nil {
		t.Fatalf("InferExternalSchema() error = %v", err)
	}
	want := []SQLRowBinaryColumn{
		{Name: "active", Type: SQLRowBinaryBool, Nullable: true},
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "name", Type: SQLRowBinaryString, Nullable: true},
		{Name: "score", Type: SQLRowBinaryFloat64},
	}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("inferred columns = %#v, want %#v", columns, want)
	}
}

func TestCH048ExternalSchemaInferenceFallsBackConservatively(t *testing.T) {
	rows := []Row{
		{"mixed": int64(1), "payload": map[string]interface{}{"region": "sg"}},
		{"mixed": "1", "payload": nil, "new": false},
	}
	columns, err := InferExternalSchema(rows, ExternalSchemaInferenceOptions{})
	if err != nil {
		t.Fatalf("InferExternalSchema() error = %v", err)
	}
	want := []SQLRowBinaryColumn{
		{Name: "mixed", Type: SQLRowBinaryJSON},
		{Name: "new", Type: SQLRowBinaryBool, Nullable: true},
		{Name: "payload", Type: SQLRowBinaryJSON, Nullable: true},
	}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("conservative columns = %#v, want %#v", columns, want)
	}
}

func TestCH048ExternalJSONSchemaInferenceKeepsLargeIntegers(t *testing.T) {
	columns, err := InferExternalJSONSchema([]byte(`[{"id":9007199254740993},{"id":9007199254740994}]`), ExternalSchemaInferenceOptions{})
	if err != nil {
		t.Fatalf("InferExternalJSONSchema() error = %v", err)
	}
	want := []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("JSON columns = %#v, want %#v", columns, want)
	}
}

func TestCH048ExternalSchemaInferenceRejectsInvalidBoundsAndEmptyInput(t *testing.T) {
	if _, err := InferExternalSchema(nil, ExternalSchemaInferenceOptions{}); err == nil {
		t.Fatal("InferExternalSchema(nil) error = nil")
	}
	if _, err := InferExternalSchema([]Row{{"id": int64(1)}}, ExternalSchemaInferenceOptions{MaxRows: -1}); err == nil {
		t.Fatal("negative MaxRows error = nil")
	}
	if _, err := InferExternalJSONSchema([]byte(`[{"id":1}]`), ExternalSchemaInferenceOptions{MaxColumns: 0}); err != nil {
		t.Fatalf("zero MaxColumns error = %v", err)
	}
	if _, err := InferExternalSchema([]Row{{"a": 1}, {"b": 2}}, ExternalSchemaInferenceOptions{MaxColumns: 1}); err == nil {
		t.Fatal("MaxColumns overflow error = nil")
	}
	if _, err := InferExternalJSONSchema([]byte(`[{"id":1},{"id":2}]`), ExternalSchemaInferenceOptions{MaxRows: 1}); err == nil {
		t.Fatal("JSON MaxRows overflow error = nil")
	}
	if _, err := InferExternalJSONSchema([]byte(`[{"id":1}] {"id":2}`), ExternalSchemaInferenceOptions{}); err == nil {
		t.Fatal("trailing JSON error = nil")
	}
	for name, options := range map[string]ExternalSchemaInferenceOptions{
		"negative MaxColumns": {MaxColumns: -1},
		"negative MaxBytes":   {MaxBytes: -1},
		"large MaxRows":       {MaxRows: maxExternalSchemaInferenceRows + 1},
		"large MaxColumns":    {MaxColumns: maxExternalSchemaInferenceColumns + 1},
		"large MaxBytes":      {MaxBytes: int64(defaultExternalSchemaInferenceMaxBytes) + 1},
	} {
		if _, err := InferExternalJSONSchema([]byte(`[{"id":1}]`), options); err == nil {
			t.Fatalf("%s error = nil", name)
		}
	}
	if _, err := InferExternalJSONSchema([]byte(`[{"id":1}]`), ExternalSchemaInferenceOptions{MaxBytes: 5}); err == nil {
		t.Fatal("JSON MaxBytes overflow error = nil")
	}
}

func TestCH048ExternalSchemaInferenceMapsSupportedScalarKinds(t *testing.T) {
	rows := []Row{{
		"bool":     true,
		"bytes":    []byte("payload"),
		"duration": time.Second,
		"float":    float64(1.5),
		"int":      int32(-1),
		"time":     time.Unix(1700000000, 0).UTC(),
		"uint":     uint64(2),
		"string":   "value",
	}}
	columns, err := InferExternalSchema(rows, ExternalSchemaInferenceOptions{})
	if err != nil {
		t.Fatalf("InferExternalSchema() error = %v", err)
	}
	want := []SQLRowBinaryColumn{
		{Name: "bool", Type: SQLRowBinaryBool},
		{Name: "bytes", Type: SQLRowBinaryBytes},
		{Name: "duration", Type: SQLRowBinaryDuration},
		{Name: "float", Type: SQLRowBinaryFloat64},
		{Name: "int", Type: SQLRowBinaryInt64},
		{Name: "string", Type: SQLRowBinaryString},
		{Name: "time", Type: SQLRowBinaryDateTime},
		{Name: "uint", Type: SQLRowBinaryUint64},
	}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("scalar columns = %#v, want %#v", columns, want)
	}
}

func TestCH048ExternalNDJSONAndRegisteredTableInference(t *testing.T) {
	columns, err := InferExternalNDJSONSchema([]byte("{\"id\":9007199254740993,\"name\":\"one\"}\n{\"id\":9007199254740994}\n"), ExternalSchemaInferenceOptions{})
	if err != nil {
		t.Fatalf("InferExternalNDJSONSchema() error = %v", err)
	}
	want := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "name", Type: SQLRowBinaryString, Nullable: true},
	}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("NDJSON columns = %#v, want %#v", columns, want)
	}

	tables := NewExternalTables()
	if err := tables.Register("events", ExternalTable{Columns: []string{"id", "ok"}, Rows: []Row{{"id": int64(1), "ok": true}}}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	columns, err = tables.InferSchema("events", ExternalSchemaInferenceOptions{})
	if err != nil {
		t.Fatalf("InferSchema() error = %v", err)
	}
	want = []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "ok", Type: SQLRowBinaryBool},
	}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("registered table columns = %#v, want %#v", columns, want)
	}
}
