package hatSql

import (
	"bytes"
	"reflect"
	"testing"
)

func TestSQLColumnarBlockStreamRoundTripsTypedBlocksAndProgress(t *testing.T) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "state", Type: SQLRowBinaryString, Nullable: true},
		{Name: "enabled", Type: SQLRowBinaryBool},
	}
	rows := []Row{
		{"id": int64(1), "state": "ready", "enabled": true},
		{"id": int64(2), "state": nil, "enabled": false},
		{"id": int64(3), "state": "done", "enabled": true},
	}

	var encoded bytes.Buffer
	writer := NewSQLColumnarBlockStreamWriterWithColumns(&encoded, columns, 2)
	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatalf("WriteRow() error = %v", err)
		}
	}
	if got := writer.Progress(); got != (SQLColumnarBlockStreamProgress{Blocks: 1, Rows: 2}) {
		t.Fatalf("writer progress before Finish = %#v", got)
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	if got := writer.Progress(); got != (SQLColumnarBlockStreamProgress{Blocks: 2, Rows: 3}) {
		t.Fatalf("writer progress after Finish = %#v", got)
	}

	reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatalf("NewSQLColumnarBlockStreamReader() error = %v", err)
	}
	if got := reader.Columns(); !reflect.DeepEqual(got, columns) {
		t.Fatalf("Columns() = %#v, want %#v", got, columns)
	}
	var decoded []Row
	var blocks int
	for reader.NextBlock() {
		blocks++
		decoded = append(decoded, reader.Block()...)
		wantRows := blocks * 2
		if wantRows > len(rows) {
			wantRows = len(rows)
		}
		if got := reader.Progress().Rows; got != uint64(wantRows) {
			t.Fatalf("reader progress after block %d = %d, want %d", blocks, got, wantRows)
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader Err() = %v", err)
	}
	if blocks != 2 {
		t.Fatalf("decoded blocks = %d, want 2", blocks)
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatalf("decoded rows = %#v, want %#v", decoded, rows)
	}
}

func TestSQLColumnarBlockStreamWritesEmptyTypedStream(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64, Nullable: true}}
	var encoded bytes.Buffer
	writer := NewSQLColumnarBlockStreamWriterWithColumns(&encoded, columns, 4)
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatalf("NewSQLColumnarBlockStreamReader() error = %v", err)
	}
	if reader.NextBlock() {
		t.Fatal("NextBlock() = true for empty stream")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader Err() = %v", err)
	}
	if got := reader.Progress(); got != (SQLColumnarBlockStreamProgress{}) {
		t.Fatalf("empty progress = %#v", got)
	}
}

func TestSQLColumnarBlockStreamProjectionSkipsUnrequestedColumns(t *testing.T) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "state", Type: SQLRowBinaryString},
	}
	rows := []Row{{"id": int64(7), "state": "ready"}, {"id": int64(8), "state": "done"}}
	var encoded bytes.Buffer
	writer := NewSQLColumnarBlockStreamWriterWithColumns(&encoded, columns, 1)
	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatalf("WriteRow() error = %v", err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatalf("NewSQLColumnarBlockStreamReader() error = %v", err)
	}
	var projected []Row
	for reader.NextBlockFields([]string{"state"}) {
		projected = append(projected, reader.Block()...)
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("projected reader Err() = %v", err)
	}
	if want := []Row{{"state": "ready"}, {"state": "done"}}; !reflect.DeepEqual(projected, want) {
		t.Fatalf("projected rows = %#v, want %#v", projected, want)
	}

	reader, err = NewSQLColumnarBlockStreamReader(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatalf("NewSQLColumnarBlockStreamReader() second error = %v", err)
	}
	if reader.NextBlockFields([]string{"missing"}) {
		t.Fatal("NextBlockFields() = true for an unknown column")
	}
	if err := reader.Err(); err == nil {
		t.Fatal("unknown projection did not set Err()")
	}
}

func TestSQLColumnarBlockStreamRejectsTruncationAndInvalidFrame(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}}
	var encoded bytes.Buffer
	writer := NewSQLColumnarBlockStreamWriterWithColumns(&encoded, columns, 2)
	if err := writer.WriteRow(Row{"id": int64(1)}); err != nil {
		t.Fatalf("WriteRow() error = %v", err)
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}

	truncated := encoded.Bytes()[:encoded.Len()-1]
	reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(truncated))
	if err != nil {
		t.Fatalf("NewSQLColumnarBlockStreamReader(truncated) error = %v", err)
	}
	for reader.NextBlock() {
	}
	if err := reader.Err(); err == nil {
		t.Fatal("truncated stream did not set Err()")
	}

	invalid := append([]byte(nil), encoded.Bytes()...)
	invalid[len(invalid)-1] = 2
	reader, err = NewSQLColumnarBlockStreamReader(bytes.NewReader(invalid))
	if err != nil {
		t.Fatalf("NewSQLColumnarBlockStreamReader(invalid) error = %v", err)
	}
	for reader.NextBlock() {
	}
	if err := reader.Err(); err == nil {
		t.Fatal("invalid frame did not set Err()")
	}
}

func TestSQLColumnarBlockStreamFailedRowDoesNotAdvanceProgress(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}}
	var encoded bytes.Buffer
	writer := NewSQLColumnarBlockStreamWriterWithColumns(&encoded, columns, 2)
	if err := writer.WriteRow(Row{}); err == nil {
		t.Fatal("WriteRow() accepted a NULL non-nullable value")
	}
	if got := writer.Progress(); got != (SQLColumnarBlockStreamProgress{}) {
		t.Fatalf("progress after failed row = %#v", got)
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() after failed row error = %v", err)
	}
	reader, err := NewSQLColumnarBlockStreamReader(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatalf("NewSQLColumnarBlockStreamReader() error = %v", err)
	}
	if reader.NextBlock() {
		t.Fatal("failed row produced a block")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader Err() = %v", err)
	}
}
