package hatSql_test

import (
	"bytes"
	"context"
	"io"
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

func TestExternalTablesImportExportArrow(t *testing.T) {
	tables := hatSql.NewExternalTables()
	if err := tables.ImportJSON("events", []byte(`[{"id":1,"active":true,"name":"Ada"},{"id":2,"active":false,"name":"Lin"}]`)); err != nil {
		t.Fatalf("ImportJSON() error = %v", err)
	}
	data, err := tables.ExportArrow("events")
	if err != nil {
		t.Fatalf("ExportArrow() error = %v", err)
	}
	if len(data) == 0 {
		t.Fatal("ExportArrow() returned no IPC data")
	}
	if err := tables.ImportArrow("events_arrow", data); err != nil {
		t.Fatalf("ImportArrow() error = %v", err)
	}
	table, ok := tables.Get("events_arrow")
	if !ok || !reflect.DeepEqual(table, hatSql.ExternalTable{
		Columns: []string{"active", "id", "name"},
		Rows: []hatSql.Row{
			{"id": float64(1), "active": true, "name": "Ada"},
			{"id": float64(2), "active": false, "name": "Lin"},
		},
	}) {
		t.Fatalf("Arrow table = %#v, %v", table, ok)
	}
}

func TestExternalTablesWriteArrowAndParquet(t *testing.T) {
	tables := hatSql.NewExternalTables()
	if err := tables.ImportJSON("events", []byte(`[{"id":1,"name":"Ada"},{"id":2,"name":"Lin"}]`)); err != nil {
		t.Fatalf("ImportJSON() error = %v", err)
	}
	arrowBuffer := bytes.Buffer{}
	if err := tables.WriteArrow("events", &arrowBuffer); err != nil {
		t.Fatalf("WriteArrow() error = %v", err)
	}
	if err := tables.ImportArrow("events_arrow", arrowBuffer.Bytes()); err != nil {
		t.Fatalf("ImportArrow() from WriteArrow() error = %v", err)
	}
	parquetBuffer := bytes.Buffer{}
	if err := tables.WriteParquet("events", &parquetBuffer); err != nil {
		t.Fatalf("WriteParquet() error = %v", err)
	}
	if err := tables.ImportParquet("events_parquet", parquetBuffer.Bytes()); err != nil {
		t.Fatalf("ImportParquet() from WriteParquet() error = %v", err)
	}
	wantArrow := hatSql.ExternalTable{Columns: []string{"id", "name"}, Rows: []hatSql.Row{{"id": float64(1), "name": "Ada"}, {"id": float64(2), "name": "Lin"}}}
	if table, ok := tables.Get("events_arrow"); !ok || !reflect.DeepEqual(table, wantArrow) {
		t.Fatalf("events_arrow = %#v, %v", table, ok)
	}
	wantParquet := hatSql.ExternalTable{Columns: []string{"id", "name"}, Rows: []hatSql.Row{{"id": "1", "name": "Ada"}, {"id": "2", "name": "Lin"}}}
	if table, ok := tables.Get("events_parquet"); !ok || !reflect.DeepEqual(table, wantParquet) {
		t.Fatalf("events_parquet = %#v, %v", table, ok)
	}
}

func BenchmarkExternalTablesExportTransfer(b *testing.B) {
	tables := hatSql.NewExternalTables()
	rows := make([]hatSql.Row, 10_000)
	for index := range rows {
		rows[index] = hatSql.Row{"id": int64(index), "name": "benchmark"}
	}
	if err := tables.Register("events", hatSql.ExternalTable{Columns: []string{"id", "name"}, Rows: rows}); err != nil {
		b.Fatal(err)
	}
	b.Run("Arrow/ExportBytes", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			data, err := tables.ExportArrow("events")
			if err != nil || len(data) == 0 {
				b.Fatalf("ExportArrow() = %d bytes, %v", len(data), err)
			}
		}
	})
	b.Run("Arrow/WriteDiscard", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := tables.WriteArrow("events", io.Discard); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Parquet/ExportBytes", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			data, err := tables.ExportParquet("events")
			if err != nil || len(data) == 0 {
				b.Fatalf("ExportParquet() = %d bytes, %v", len(data), err)
			}
		}
	})
	b.Run("Parquet/WriteDiscard", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := tables.WriteParquet("events", io.Discard); err != nil {
				b.Fatal(err)
			}
		}
	})
}
