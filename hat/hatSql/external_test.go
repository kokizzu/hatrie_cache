package hatSql_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestExternalTablesImportExportAndScanCSVAndJSON(t *testing.T) {
	tables := hatSql.NewExternalTables()
	if err := tables.ImportCSV("events", []byte("id,state\n1,open\n2,closed\n")); err != nil {
		t.Fatalf("ImportCSV() error = %v", err)
	}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM EXTERNAL('events') AS event
WHERE event.state = 'open'
SELECT event.id`, tables, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("external CSV query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []hatSql.Row{{"id": "1"}}) {
		t.Fatalf("external CSV query rows = %#v", result.Rows)
	}
	exported, err := tables.ExportCSV("events")
	if err != nil || string(exported) != "id,state\n1,open\n2,closed\n" {
		t.Fatalf("ExportCSV() = %q, %v", exported, err)
	}
	if err := tables.ImportJSON("profiles", []byte(`[{"id":1,"name":"Ada"}]`)); err != nil {
		t.Fatalf("ImportJSON() error = %v", err)
	}
	profiles, ok := tables.Get("profiles")
	if !ok || !reflect.DeepEqual(profiles.Rows, []hatSql.Row{{"id": float64(1), "name": "Ada"}}) {
		t.Fatalf("profiles = %#v, %v", profiles, ok)
	}
	jsonBytes, err := tables.ExportJSON("profiles")
	if err != nil || string(jsonBytes) != `[{"id":1,"name":"Ada"}]` {
		t.Fatalf("ExportJSON() = %q, %v", jsonBytes, err)
	}
}
