package hatSql_test

import (
	"bytes"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryParallelDecodeMatchesSerial(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "name", Type: hatSql.SQLRowBinaryString},
		{Name: "active", Type: hatSql.SQLRowBinaryBool},
		{Name: "note", Type: hatSql.SQLRowBinaryBytes, Nullable: true},
	}
	rows := make([]hatSql.SQLRow, 4096)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":     int64(index),
			"name":   "row-" + string(rune('a'+index%26)),
			"active": index%2 == 0,
		}
		if index%5 != 0 {
			rows[index]["note"] = []byte("payload")
		}
	}
	wire, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := hatSql.DecodeSQLRowBinary(columns, wire)
	if err != nil {
		t.Fatal(err)
	}
	parallel, err := hatSql.DecodeSQLRowBinaryParallel(columns, wire)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parallel, serial) {
		t.Fatalf("parallel decode differs from serial decode")
	}
	if len(parallel) != len(rows) || !bytes.Equal(parallel[1]["note"].([]byte), []byte("payload")) {
		t.Fatalf("parallel decode rows = %d, want %d with copied byte payload", len(parallel), len(rows))
	}
}

func TestSQLRowBinaryParallelDecodeRejectsMalformedInput(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	wire, err := hatSql.EncodeSQLRowBinary(columns, []hatSql.SQLRow{{"id": int64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	for name, corrupted := range map[string][]byte{
		"truncated": wire[:len(wire)-1],
		"trailing":  append(append([]byte(nil), wire...), 0),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := hatSql.DecodeSQLRowBinaryParallel(columns, corrupted); err == nil {
				t.Fatal("DecodeSQLRowBinaryParallel() error = nil")
			}
		})
	}
}
