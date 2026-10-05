package hatSql

import (
	"reflect"
	"strings"
	"testing"
)

func TestCH047CSVParallelPreservesRFC4180Order(t *testing.T) {
	data := []byte("id,state,note\r\n1,open,\"line one\r\nline two\"\r\n2,closed,\"comma, stays quoted\"\r\n")
	columns, rows, err := ParseCSVParallel(data, ExternalImportOptions{}, 2)
	if err != nil {
		t.Fatalf("ParseCSVParallel() error = %v", err)
	}
	if want := []string{"id", "state", "note"}; !reflect.DeepEqual(columns, want) {
		t.Fatalf("columns = %#v, want %#v", columns, want)
	}
	want := []Row{
		{"id": "1", "state": "open", "note": "line one\nline two"},
		{"id": "2", "state": "closed", "note": "comma, stays quoted"},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %#v, want %#v", rows, want)
	}
}

func TestCH047CSVParallelReturnsLowestRecordErrorAndKeepsTableAtomic(t *testing.T) {
	tables := NewExternalTables()
	if err := tables.Register("events", ExternalTable{Columns: []string{"id"}, Rows: []Row{{"id": "old"}}}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	data := []byte("id\n1\nnot,valid\n\"unterminated\n")
	if err := tables.ImportCSVParallel("events", data, ExternalImportOptions{}, 3); err == nil || !strings.Contains(err.Error(), "CSV record 3") {
		t.Fatalf("ImportCSVParallel() error = %v, want lowest malformed record", err)
	}
	table, ok := tables.Get("events")
	if !ok || !reflect.DeepEqual(table.Rows, []Row{{"id": "old"}}) {
		t.Fatalf("table after failed import = %#v/%v, want original table", table, ok)
	}
}
