package hatSql_test

import (
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCH047ParallelCSVPreservesRFC4180RecordsAndOrder(t *testing.T) {
	data := "id,name,note\n1,Ada,\"first line\nsecond line\"\n2,Bea,plain\n3,Cal,\"comma, value\"\n"
	got, err := hatSql.ParseCSVParallel(strings.NewReader(data), 2)
	if err != nil {
		t.Fatalf("ParseCSVParallel() error = %v", err)
	}
	want := []hatSql.Row{
		{"id": "1", "name": "Ada", "note": "first line\nsecond line"},
		{"id": "2", "name": "Bea", "note": "plain"},
		{"id": "3", "name": "Cal", "note": "comma, value"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseCSVParallel() rows = %#v, want %#v", got, want)
	}
}

func TestCH047ParallelCSVRejectsMalformedRecordDeterministically(t *testing.T) {
	data := "id,note\n1,ok\n2,\"unterminated\n3,later\n"
	if _, err := hatSql.ParseCSVParallel(strings.NewReader(data), 3); err == nil || !strings.Contains(err.Error(), "record 3") {
		t.Fatalf("ParseCSVParallel() error = %v, want record parse error", err)
	}
}

func TestCH047ParallelCSVHandlesEscapedQuotesCRLFAndHeaderOnlyInput(t *testing.T) {
	data := "id,note\r\n1,\"say \"\"hello\"\"\"\r\n\r\n"
	got, err := hatSql.ParseCSVParallel(strings.NewReader(data), 0)
	if err != nil {
		t.Fatalf("ParseCSVParallel() error = %v", err)
	}
	want := []hatSql.Row{{"id": "1", "note": "say \"hello\""}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseCSVParallel() rows = %#v, want %#v", got, want)
	}

	tables := hatSql.NewExternalTables()
	if err := tables.ImportCSVParallel("empty", []byte("id,note\r\n"), 2); err != nil {
		t.Fatalf("ImportCSVParallel() header-only error = %v", err)
	}
	table, ok := tables.Get("empty")
	if !ok || !reflect.DeepEqual(table.Columns, []string{"id", "note"}) || len(table.Rows) != 0 {
		t.Fatalf("header-only table = %#v, %t", table, ok)
	}
}

func TestCH047ParallelCSVImportIsAtomicOnFailure(t *testing.T) {
	tables := hatSql.NewExternalTables()
	if err := tables.ImportCSVParallel("events", []byte("id,note\n1,old\n"), 2); err != nil {
		t.Fatalf("initial ImportCSVParallel() error = %v", err)
	}
	if err := tables.ImportCSVParallel("events", []byte("id,note\n2,\"bad\n"), 2); err == nil {
		t.Fatal("ImportCSVParallel() accepted malformed input")
	}
	table, ok := tables.Get("events")
	if !ok || len(table.Rows) != 1 || table.Rows[0]["id"] != "1" {
		t.Fatalf("failed import replaced existing table = %#v, %t", table, ok)
	}
}

func TestCH047ParallelCSVImportRejectsNilTablesBeforeParsing(t *testing.T) {
	var tables *hatSql.ExternalTables
	if err := tables.ImportCSVParallel("events", []byte("id\n1\n"), 2); err == nil || !strings.Contains(err.Error(), "external tables are nil") {
		t.Fatalf("ImportCSVParallel() nil receiver error = %v", err)
	}
}
