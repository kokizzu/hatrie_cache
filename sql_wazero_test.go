package hatriecache

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestSQLWASMFunctionNumericABI(t *testing.T) {
	// (module (func (export "plus_one") (param i64) (result i64) local.get 0 i64.const 1 i64.add))
	wasm := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x06, 0x01, 0x60, 0x01, 0x7e, 0x01, 0x7e, 0x03, 0x02, 0x01, 0x00, 0x07, 0x0c, 0x01, 0x08, 'p', 'l', 'u', 's', '_', 'o', 'n', 'e', 0x00, 0x00, 0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x42, 0x01, 0x7c, 0x0b}
	definition, err := CompileSQLFunction("CREATE FUNCTION plus_one(value INTEGER) LANGUAGE WASM AS '" + base64.StdEncoding.EncodeToString(wasm) + "'")
	if err != nil {
		t.Fatal(err)
	}
	registry := NewSQLFunctionRegistry()
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	values, err := registry.EvaluateSQLFunction("plus_one", []SQLFunctionCall{{Arguments: []interface{}{int64(41)}}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := values[0], int64(42); got != want {
		t.Fatalf("result = %#v, want %#v", got, want)
	}
}

func TestSQLWASMFunctionRejectsUnsupportedArgumentType(t *testing.T) {
	_, err := CompileSQLFunction("CREATE FUNCTION nope(value TEXT) LANGUAGE WASM AS 'AGFzbQEAAAA='")
	if err == nil || !strings.Contains(err.Error(), "must be INTEGER, NUMBER, or BOOLEAN") {
		t.Fatalf("error = %v, want clear ABI type error", err)
	}
}

func TestSQLFunctionRegistryCloseReleasesWASMRuntime(t *testing.T) {
	wasm := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x06, 0x01, 0x60, 0x01, 0x7e, 0x01, 0x7e, 0x03, 0x02, 0x01, 0x00, 0x07, 0x0c, 0x01, 0x08, 'p', 'l', 'u', 's', '_', 'o', 'n', 'e', 0x00, 0x00, 0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x42, 0x01, 0x7c, 0x0b}
	registry := NewSQLFunctionRegistry()
	if err := registry.Register(SQLFunctionDefinition{Name: "plus_one", Arguments: []string{"value"}, ArgumentTypes: []string{"INTEGER"}, Language: "WASM", Source: base64.StdEncoding.EncodeToString(wasm)}); err != nil {
		t.Fatal(err)
	}
	registry.Close()
	if _, err := registry.EvaluateSQLFunction("plus_one", nil); err == nil || !strings.Contains(err.Error(), "unknown SQL function") {
		t.Fatalf("EvaluateSQLFunction after Close() error = %v, want unknown function", err)
	}
}
