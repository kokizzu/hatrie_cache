package hatSql

import (
	"encoding/json"
	"testing"
)

var tr018CopiedRowBinarySink []SQLRow

func BenchmarkTR018CopiedRowBinaryDecode(b *testing.B) {
	columns := tr018RowBinaryColumns()
	rows := tr018RowBinaryRows(512)
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		decoded, err := DecodeSQLRowBinary(columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
		tr018CopiedRowBinarySink = decoded
	}
}

func tr018RowBinaryColumns() []SQLRowBinaryColumn {
	return []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64, Nullable: true},
		{Name: "name", Type: SQLRowBinaryString, Nullable: true},
		{Name: "payload", Type: SQLRowBinaryBytes, Nullable: true},
		{Name: "document", Type: SQLRowBinaryJSON, Nullable: true},
		{Name: "optional", Type: SQLRowBinaryString, Nullable: true},
	}
}

func tr018RowBinaryRows(count int) []SQLRow {
	rows := make([]SQLRow, count)
	for index := range rows {
		rows[index] = SQLRow{
			"id":       int64(index),
			"name":     "borrowed-row-name-" + string(rune('a'+index%26)),
			"payload":  []byte("borrowed-row-payload"),
			"document": json.RawMessage(`{"kind":"row","active":true}`),
		}
		if index%7 == 0 {
			rows[index]["optional"] = "optional-value"
		}
	}
	return rows
}
