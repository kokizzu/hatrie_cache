package hatSql_test

import (
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestParseNDJSONParallelPreservesOrderAndReportsFirstError(t *testing.T) {
	data := []byte("\n{\"id\":1}\nnot-json\n{\"id\":4}\nnull\n{\"id\":6}\n")
	rows, err := hatSql.ParseNDJSONParallel(data, 4)
	if err == nil || !strings.Contains(err.Error(), "record 3") {
		t.Fatalf("ParseNDJSONParallel() error = %v, want deterministic first error on record 3", err)
	}
	if rows != nil {
		t.Fatalf("ParseNDJSONParallel() rows = %#v after error, want nil", rows)
	}

	rows, err = hatSql.ParseNDJSONParallel([]byte("{\"id\":1}\n{\"id\":2}\n"), 0)
	if err != nil {
		t.Fatalf("ParseNDJSONParallel() valid input error = %v", err)
	}
	want := []hatSql.Row{{"id": float64(1)}, {"id": float64(2)}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("ParseNDJSONParallel() rows = %#v, want %#v", rows, want)
	}
}

func TestImportNDJSONParallelIsAtomic(t *testing.T) {
	tables := hatSql.NewExternalTables()
	if err := tables.ImportNDJSON("events", []byte("{\"id\":1}\n")); err != nil {
		t.Fatal(err)
	}
	if err := tables.ImportNDJSONParallel("events", []byte("{\"id\":2}\nbad\n{\"id\":3}\n"), 3); err == nil {
		t.Fatal("ImportNDJSONParallel() accepted malformed input")
	}
	table, ok := tables.Get("events")
	if !ok || !reflect.DeepEqual(table.Rows, []hatSql.Row{{"id": float64(1)}}) {
		t.Fatalf("failed parallel import changed table = %#v, %v", table, ok)
	}
	if err := tables.ImportNDJSONParallel("events", []byte("{\"id\":2}\n{\"id\":3}\n"), 1); err != nil {
		t.Fatalf("ImportNDJSONParallel() valid input error = %v", err)
	}
	table, ok = tables.Get("events")
	if !ok || !reflect.DeepEqual(table.Rows, []hatSql.Row{{"id": float64(2)}, {"id": float64(3)}}) {
		t.Fatalf("parallel import rows = %#v, %v", table.Rows, ok)
	}
}

func BenchmarkExternalTablesImportNDJSONParallel(b *testing.B) {
	data := []byte(strings.Repeat("{\"id\":1,\"name\":\"benchmark\"}\n", 10_000))
	b.SetBytes(int64(len(data)))
	b.Run("sequential", func(b *testing.B) {
		b.ReportAllocs()
		tables := hatSql.NewExternalTables()
		for range b.N {
			if err := tables.ImportNDJSON("events", data); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel-4", func(b *testing.B) {
		b.ReportAllocs()
		tables := hatSql.NewExternalTables()
		for range b.N {
			if err := tables.ImportNDJSONParallel("events", data, 4); err != nil {
				b.Fatal(err)
			}
		}
	})
}
