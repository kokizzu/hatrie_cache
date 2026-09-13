package hatSql

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestSQLRowBinaryEnumRoundTrip(t *testing.T) {
	columns := []SQLRowBinaryColumn{{
		Name:       "status",
		Type:       SQLRowBinaryEnum8,
		EnumValues: []string{"queued", "running", "done"},
	}}
	rows := []SQLRow{
		{"status": "queued"},
		{"status": SQLEnum8(1)},
		{"status": SQLEnum8(2)},
	}

	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinary() error = %v", err)
	}
	if want := []byte{0, 1, 2}; !bytes.Equal(encoded, want) {
		t.Fatalf("encoded = %v, want %v", encoded, want)
	}
	decoded, err := DecodeSQLRowBinary(columns, encoded)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinary() error = %v", err)
	}
	want := []SQLRow{{"status": SQLEnum8(0)}, {"status": SQLEnum8(1)}, {"status": SQLEnum8(2)}}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("decoded = %#v, want %#v", decoded, want)
	}
}

func TestSQLRowBinaryEnum16UsesLittleEndianCodes(t *testing.T) {
	labels := make([]string, 301)
	for index := range labels {
		labels[index] = fmt.Sprintf("value-%d", index)
	}
	columns := []SQLRowBinaryColumn{{Name: "code", Type: SQLRowBinaryEnum16, EnumValues: labels}}
	rows := []SQLRow{{"code": SQLEnum16(255)}, {"code": SQLEnum16(300)}}

	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinary() error = %v", err)
	}
	if want := []byte{0xff, 0x00, 0x2c, 0x01}; !bytes.Equal(encoded, want) {
		t.Fatalf("encoded = %v, want %v", encoded, want)
	}
	decoded, err := DecodeSQLRowBinary(columns, encoded)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinary() error = %v", err)
	}
	want := []SQLRow{{"code": SQLEnum16(255)}, {"code": SQLEnum16(300)}}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("decoded = %#v, want %#v", decoded, want)
	}
}

func TestSQLRowBinaryEnumValidationRejectsInvalidSchemasAndValues(t *testing.T) {
	tooManyEnum8 := make([]string, 257)
	for index := range tooManyEnum8 {
		tooManyEnum8[index] = fmt.Sprintf("value-%d", index)
	}
	cases := []struct {
		name    string
		columns []SQLRowBinaryColumn
		rows    []SQLRow
	}{
		{name: "missing labels", columns: []SQLRowBinaryColumn{{Name: "status", Type: SQLRowBinaryEnum8}}},
		{name: "duplicate labels", columns: []SQLRowBinaryColumn{{Name: "status", Type: SQLRowBinaryEnum8, EnumValues: []string{"same", "same"}}}},
		{name: "too many enum8 labels", columns: []SQLRowBinaryColumn{{Name: "status", Type: SQLRowBinaryEnum8, EnumValues: tooManyEnum8}}},
		{name: "labels on non enum", columns: []SQLRowBinaryColumn{{Name: "status", Type: SQLRowBinaryString, EnumValues: []string{"same"}}}},
		{name: "unknown label", columns: []SQLRowBinaryColumn{{Name: "status", Type: SQLRowBinaryEnum8, EnumValues: []string{"queued"}}}, rows: []SQLRow{{"status": "missing"}}},
		{name: "code outside labels", columns: []SQLRowBinaryColumn{{Name: "status", Type: SQLRowBinaryEnum8, EnumValues: []string{"queued"}}}, rows: []SQLRow{{"status": SQLEnum8(1)}}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := EncodeSQLRowBinary(testCase.columns, testCase.rows); err == nil {
				t.Fatal("EncodeSQLRowBinary() error = nil, want error")
			}
		})
	}
}

func TestSQLRowBinaryEnumDecodeRejectsUnknownCode(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "status", Type: SQLRowBinaryEnum8, EnumValues: []string{"queued", "done"}}}
	if _, err := DecodeSQLRowBinary(columns, []byte{2}); err == nil {
		t.Fatal("DecodeSQLRowBinary() error = nil, want unknown enum code error")
	}
	if _, err := AnalyzeSQLRowBinaryRead(columns, []byte{2}); err == nil {
		t.Fatal("AnalyzeSQLRowBinaryRead() error = nil, want unknown enum code error")
	}
}

func TestSQLRowBinaryEnumDeltaRejectsOverflowBeforeNarrowing(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "status", Type: SQLRowBinaryEnum8, EnumValues: []string{"queued", "done"}}}
	encoded := []byte{'H', 'S', 'D', '1', 1, 0xd8, 0x04}
	if _, err := DecodeSQLRowBinaryDelta(columns, encoded); err == nil {
		t.Fatal("DecodeSQLRowBinaryDelta() error = nil, want enum overflow error")
	}
}

func TestSQLRowBinaryEnumStatsPruningAcceptsLabels(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "status", Type: SQLRowBinaryEnum8, EnumValues: []string{"queued", "done"}}}
	rows := []SQLRow{{"status": SQLEnum8(0)}, {"status": SQLEnum8(1)}}
	stats, err := BuildSQLRowBinaryColumnStats(columns, rows)
	if err != nil {
		t.Fatalf("BuildSQLRowBinaryColumnStats() error = %v", err)
	}
	skip, err := CanSkipSQLRowBinaryStats(columns, stats, SQLRowBinaryStatsPredicate{Column: "status", Operator: SQLRowBinaryStatsEqual, Value: "queued"})
	if err != nil {
		t.Fatalf("CanSkipSQLRowBinaryStats() error = %v", err)
	}
	if skip {
		t.Fatal("CanSkipSQLRowBinaryStats() = true, want queued to remain possible")
	}
}

func TestSQLRowBinaryEnumCompactLargePayloadUsesParallelDecoder(t *testing.T) {
	columns, rows := sqlRowBinaryEnumBenchmarkTypedFixture()
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinary() error = %v", err)
	}
	if !sqlRowBinaryShouldDecodeParallel(columns, encoded) {
		t.Fatal("sqlRowBinaryShouldDecodeParallel() = false, want compact large enum payload to use parallel decoder")
	}

	smallRows := rows[:8]
	smallEncoded, err := EncodeSQLRowBinary(columns, smallRows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinary(small) error = %v", err)
	}
	if sqlRowBinaryShouldDecodeParallel(columns, smallEncoded) {
		t.Fatal("sqlRowBinaryShouldDecodeParallel(small) = true, want serial decoder")
	}
}

func TestSQLRowBinaryEnumRoundTripsAcrossBitmapAdaptiveAndStats(t *testing.T) {
	columns := []SQLRowBinaryColumn{{
		Name:       "status",
		Type:       SQLRowBinaryEnum8,
		Nullable:   true,
		EnumValues: []string{"queued", "running", "done"},
	}}
	rows := []SQLRow{{"status": SQLEnum8(0)}, {"status": nil}, {"status": SQLEnum8(2)}}
	want := []SQLRow{{"status": SQLEnum8(0)}, {"status": nil}, {"status": SQLEnum8(2)}}

	bitmap, err := EncodeSQLRowBinaryBitmap(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryBitmap() error = %v", err)
	}
	decodedBitmap, err := DecodeSQLRowBinaryBitmap(columns, bitmap)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryBitmap() error = %v", err)
	}
	if !reflect.DeepEqual(decodedBitmap, want) {
		t.Fatalf("bitmap decoded = %#v, want %#v", decodedBitmap, want)
	}

	adaptive, err := EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryAdaptive() error = %v", err)
	}
	decodedAdaptive, err := DecodeSQLRowBinaryAdaptive(columns, adaptive)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryAdaptive() error = %v", err)
	}
	if !reflect.DeepEqual(decodedAdaptive, want) {
		t.Fatalf("adaptive decoded = %#v, want %#v", decodedAdaptive, want)
	}

	withStats, err := EncodeSQLRowBinaryWithStats(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryWithStats() error = %v", err)
	}
	decodedStatsRows, stats, err := DecodeSQLRowBinaryWithStats(columns, withStats)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryWithStats() error = %v", err)
	}
	if !reflect.DeepEqual(decodedStatsRows, want) {
		t.Fatalf("stats decoded = %#v, want %#v", decodedStatsRows, want)
	}
	if len(stats) != 1 || stats[0].NullCount != 1 || stats[0].ValueCount != 2 || stats[0].Min != SQLEnum8(0) || stats[0].Max != SQLEnum8(2) {
		t.Fatalf("stats = %#v, want one enum range [0, 2] and one NULL", stats)
	}
}

func TestSQLRowBinaryEnumStreamRoundTrip(t *testing.T) {
	var encoded bytes.Buffer
	writer := NewSQLRowBinaryStreamWriter(&encoded, []string{"status"})
	if err := writer.WriteRow(Row{"status": SQLEnum8(0)}); err != nil {
		t.Fatalf("WriteRow(first) error = %v", err)
	}
	if err := writer.WriteRow(Row{"status": SQLEnum8(1)}); err != nil {
		t.Fatalf("WriteRow(second) error = %v", err)
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	columns, rows, err := DecodeSQLRowBinaryStream(encoded.Bytes())
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryStream() error = %v", err)
	}
	if len(columns) != 1 || columns[0].Type != SQLRowBinaryEnum8 {
		t.Fatalf("columns = %#v, want one Enum8 column", columns)
	}
	want := []SQLRow{{"status": SQLEnum8(0)}, {"status": SQLEnum8(1)}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %#v, want %#v", rows, want)
	}
}
