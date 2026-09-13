package hatSchema

import "testing"

func TestSQLIPSchemaTypesValidateAndGenerate(t *testing.T) {
	for _, columnType := range []Type{TypeIPv4, TypeIPv6} {
		if err := validateColumn(Column{Name: "address", Type: columnType}); err != nil {
			t.Errorf("validateColumn(%q) error = %v", columnType, err)
		}
	}
	if got, importPath, err := modelGoType(TypeIPv4); err != nil || got != "hatSql.SQLIPv4" || importPath != "hatrie_cache/hat/hatSql" {
		t.Fatalf("modelGoType(IPV4) = %q, %q, %v", got, importPath, err)
	}
	if got, importPath, err := modelGoType(TypeIPv6); err != nil || got != "hatSql.SQLIPv6" || importPath != "hatrie_cache/hat/hatSql" {
		t.Fatalf("modelGoType(IPV6) = %q, %q, %v", got, importPath, err)
	}
}
