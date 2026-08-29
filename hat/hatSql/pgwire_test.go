package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatPgWire"
	"hatrie_cache/hat/hatSql"
)

func TestPgWireQueryHandlerExecutesSQLResult(t *testing.T) {
	handler := hatSql.NewPgWireQueryHandler(nil, hatSql.QueryOptions{})
	result, err := handler.Query(context.Background(), `FROM VALUES (1, 'Ada', TRUE) AS people(id, name, active) SELECT people.id, people.name, people.active`)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(result.Fields) != 3 || result.Fields[0].Name != "id" || result.Fields[1].Name != "name" || result.Fields[2].Name != "active" {
		t.Fatalf("fields = %#v", result.Fields)
	}
	if result.Fields[0].DataTypeOID == 0 || result.Fields[1].DataTypeOID != hatPgWire.OIDText || result.Fields[2].DataTypeOID != hatPgWire.OIDBool {
		t.Fatalf("field types = %#v", result.Fields)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 3 || result.Rows[0][0] == nil || result.Rows[0][1] == nil || result.Rows[0][2] == nil {
		t.Fatalf("rows = %#v", result.Rows)
	}
	if *result.Rows[0][0] != "1" || *result.Rows[0][1] != "Ada" || *result.Rows[0][2] != "t" {
		t.Fatalf("row = %#v, want 1 Ada t", result.Rows[0])
	}
}
