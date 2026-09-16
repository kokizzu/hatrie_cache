package hatSql

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestExternalTablesImportCSVReaderStreamsAndEnforcesLimits(t *testing.T) {
	tables := NewExternalTables()
	err := tables.ImportCSVReader("people", strings.NewReader("id,name\n1,Ada\n2,Bea\n"), ExternalImportOptions{MaxRows: 2, MaxBytes: 64})
	if err != nil {
		t.Fatalf("ImportCSVReader() error = %v", err)
	}
	table, ok := tables.Get("people")
	if !ok || !reflect.DeepEqual(table.Columns, []string{"id", "name"}) || !reflect.DeepEqual(table.Rows, []Row{{"id": "1", "name": "Ada"}, {"id": "2", "name": "Bea"}}) {
		t.Fatalf("imported CSV table = %#v, %t", table, ok)
	}
	if err := tables.ImportCSVReader("people", strings.NewReader("id\n1\n2\n"), ExternalImportOptions{MaxRows: 1}); err == nil {
		t.Fatal("ImportCSVReader() accepted too many rows")
	}
	table, ok = tables.Get("people")
	if !ok || len(table.Rows) != 2 {
		t.Fatalf("failed CSV import replaced existing table: %#v, %t", table, ok)
	}
	if err := tables.ImportCSVReader("empty", strings.NewReader("id,name\n"), ExternalImportOptions{}); err != nil {
		t.Fatalf("header-only ImportCSVReader() error = %v", err)
	}
	table, ok = tables.Get("empty")
	if !ok || !reflect.DeepEqual(table.Columns, []string{"id", "name"}) || len(table.Rows) != 0 {
		t.Fatalf("header-only CSV table = %#v, %t", table, ok)
	}
}

func TestExternalTablesImportJSONEachRowReaderIsAtomicAndBounded(t *testing.T) {
	tables := NewExternalTables()
	if err := tables.ImportNDJSON("people", []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	err := tables.ImportJSONEachRowReader("people", strings.NewReader("{\"id\":2}\nnot-json\n"), ExternalImportOptions{MaxRows: 3, MaxRecordBytes: 64})
	if err == nil {
		t.Fatal("ImportJSONEachRowReader() accepted malformed input")
	}
	table, ok := tables.Get("people")
	if !ok || !reflect.DeepEqual(table.Rows, []Row{{"id": float64(1)}}) {
		t.Fatalf("failed JSONEachRow import replaced existing table: %#v, %t", table, ok)
	}
	if err := tables.ImportJSONEachRowReader("people", strings.NewReader("{\"id\":2}\n{\"id\":3}\n"), ExternalImportOptions{MaxRows: 1}); err == nil {
		t.Fatal("ImportJSONEachRowReader() accepted too many rows")
	}
	if err := tables.ImportNDJSONReader("people2", strings.NewReader("{\"id\":4}\n"), ExternalImportOptions{}); err != nil {
		t.Fatalf("ImportNDJSONReader() error = %v", err)
	}
}

func TestStreamJSONEachRowAndCSVCallbacksStopWithoutMaterializing(t *testing.T) {
	var jsonRows []Row
	err := StreamJSONEachRow(strings.NewReader("{\"id\":1}\n{\"id\":2}\n"), ExternalImportOptions{}, func(row Row) error {
		jsonRows = append(jsonRows, row)
		return nil
	})
	if err != nil || len(jsonRows) != 2 {
		t.Fatalf("StreamJSONEachRow() = rows=%#v, error=%v", jsonRows, err)
	}
	var csvRows [][]string
	stop := errors.New("stop")
	err = StreamCSV(strings.NewReader("id,name\n1,Ada\n2,Bea\n"), ExternalImportOptions{}, func(columns []string, record []string) error {
		values := append([]string(nil), columns...)
		values = append(values, record...)
		csvRows = append(csvRows, values)
		return stop
	})
	if !errors.Is(err, stop) || len(csvRows) != 1 {
		t.Fatalf("StreamCSV() = rows=%#v, error=%v; want callback stop", csvRows, err)
	}
	if err := StreamJSONEachRow(strings.NewReader("{\"id\":123456789}\n"), ExternalImportOptions{MaxRecordBytes: 8}, func(Row) error { return nil }); err == nil {
		t.Fatal("StreamJSONEachRow() accepted an oversized record")
	}
	if err := StreamCSV(strings.NewReader("id\n1\n"), ExternalImportOptions{MaxBytes: 2}, func([]string, []string) error { return nil }); err == nil {
		t.Fatal("StreamCSV() accepted input beyond MaxBytes")
	}
	if err := StreamCSV(strings.NewReader("id\n1\n"), ExternalImportOptions{MaxRows: -1}, func([]string, []string) error { return nil }); err == nil {
		t.Fatal("StreamCSV() accepted negative MaxRows")
	}
}
