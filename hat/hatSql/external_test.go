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
	parquetBytes, err := tables.ExportParquet("events")
	if err != nil {
		t.Fatalf("ExportParquet() error = %v", err)
	}
	if err := tables.ImportParquet("events_parquet", parquetBytes); err != nil {
		t.Fatalf("ImportParquet() error = %v", err)
	}
	parquetTable, ok := tables.Get("events_parquet")
	if !ok || !reflect.DeepEqual(parquetTable, hatSql.ExternalTable{
		Columns: []string{"id", "state"},
		Rows:    []hatSql.Row{{"id": "1", "state": "open"}, {"id": "2", "state": "closed"}},
	}) {
		t.Fatalf("Parquet table = %#v, %v", parquetTable, ok)
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

func TestExternalTablesImportExportNDJSON(t *testing.T) {
	tables := hatSql.NewExternalTables()
	data := []byte("{\"id\":1,\"state\":\"open\"}\n{\"id\":2,\"state\":\"closed\"}\n")
	if err := tables.ImportNDJSON("events", data); err != nil {
		t.Fatalf("ImportNDJSON() error = %v", err)
	}
	table, ok := tables.Get("events")
	if !ok || !reflect.DeepEqual(table, hatSql.ExternalTable{
		Columns: []string{"id", "state"},
		Rows: []hatSql.Row{
			{"id": float64(1), "state": "open"},
			{"id": float64(2), "state": "closed"},
		},
	}) {
		t.Fatalf("NDJSON table = %#v, %v", table, ok)
	}
	exported, err := tables.ExportNDJSON("events")
	if err != nil || string(exported) != string(data) {
		t.Fatalf("ExportNDJSON() = %q, %v", exported, err)
	}
	if err := tables.ImportNDJSON("events", []byte("{\"id\":3}\ninvalid\n")); err == nil {
		t.Fatal("ImportNDJSON() accepted malformed second record")
	}
	afterFailure, ok := tables.Get("events")
	if !ok || !reflect.DeepEqual(afterFailure, table) {
		t.Fatalf("failed import changed table = %#v, want %#v", afterFailure, table)
	}
}
