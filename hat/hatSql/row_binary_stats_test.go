package hatSql_test

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryStatsRoundTripAndExactMetadata(t *testing.T) {
	columns := statsTestColumns()
	rows := []hatSql.SQLRow{
		{
			"id":       int64(7),
			"count":    uint64(20),
			"score":    2.5,
			"active":   false,
			"label":    "beta",
			"blob":     []byte{0x02, 0x03},
			"day":      time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC),
			"at":       time.Date(2026, time.January, 3, 12, 0, 0, 0, time.UTC),
			"ttl":      5 * time.Second,
			"uuid":     [16]byte{0x02},
			"metadata": json.RawMessage(`{"tier":"warm"}`),
			"optional": "present",
		},
		{
			"id":       int64(3),
			"count":    uint64(10),
			"score":    1.5,
			"active":   true,
			"label":    "alpha",
			"blob":     []byte{0x01, 0x04},
			"day":      time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
			"at":       time.Date(2026, time.January, 1, 8, 0, 0, 0, time.UTC),
			"ttl":      2 * time.Second,
			"uuid":     [16]byte{0x01, 0xff},
			"metadata": json.RawMessage(`{"tier":"cold"}`),
			"optional": nil,
		},
	}
	wantStats, err := hatSql.BuildSQLRowBinaryColumnStats(columns, rows)
	if err != nil {
		t.Fatalf("build stats: %v", err)
	}
	wire, err := hatSql.EncodeSQLRowBinaryWithStats(columns, rows)
	if err != nil {
		t.Fatalf("encode with stats: %v", err)
	}
	gotRows, gotStats, err := hatSql.DecodeSQLRowBinaryWithStats(columns, wire)
	if err != nil {
		t.Fatalf("decode with stats: %v", err)
	}
	if !reflect.DeepEqual(gotRows, rows) {
		t.Fatalf("rows mismatch: got %#v want %#v", gotRows, rows)
	}
	if !reflect.DeepEqual(gotStats, wantStats) {
		t.Fatalf("stats mismatch: got %#v want %#v", gotStats, wantStats)
	}
	if gotStats[10].HasMinMax {
		t.Fatal("JSON column unexpectedly has min/max stats")
	}
	if gotStats[11].NullCount != 1 || gotStats[11].ValueCount != 1 {
		t.Fatalf("nullable counts = %#v", gotStats[11])
	}
}

func TestSQLRowBinaryStatsHandlesNaNAndEmptyRows(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "score", Type: hatSql.SQLRowBinaryFloat64, Nullable: true}}
	rows := []hatSql.SQLRow{{"score": math.NaN()}, {"score": nil}, {"score": math.Inf(1)}}
	stats, err := hatSql.BuildSQLRowBinaryColumnStats(columns, rows)
	if err != nil {
		t.Fatalf("build NaN stats: %v", err)
	}
	if !stats[0].HasMinMax || stats[0].NullCount != 1 || stats[0].ValueCount != 2 || stats[0].Min != math.Inf(1) || stats[0].Max != math.Inf(1) {
		t.Fatalf("unexpected NaN stats: %#v", stats[0])
	}
	emptyWire, err := hatSql.EncodeSQLRowBinaryWithStats(columns, nil)
	if err != nil {
		t.Fatalf("encode empty stats: %v", err)
	}
	gotRows, gotStats, err := hatSql.DecodeSQLRowBinaryWithStats(columns, emptyWire)
	if err != nil {
		t.Fatalf("decode empty stats: %v", err)
	}
	if len(gotRows) != 0 || gotStats[0].NullCount != 0 || gotStats[0].ValueCount != 0 {
		t.Fatalf("unexpected empty result: rows=%#v stats=%#v", gotRows, gotStats)
	}
}

func TestSQLRowBinaryStatsRejectsCorruptionAndStaleMetadata(t *testing.T) {
	columns := statsTestColumns()
	rows := []hatSql.SQLRow{{"id": int64(1), "count": uint64(1), "score": 1.0, "active": true, "label": "a", "blob": []byte("a"), "day": time.Unix(0, 0).UTC(), "at": time.Unix(0, 0).UTC(), "ttl": time.Second, "uuid": [16]byte{1}, "metadata": json.RawMessage(`{}`), "optional": "x"}}
	wire, err := hatSql.EncodeSQLRowBinaryWithStats(columns, rows)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for _, corrupted := range [][]byte{
		wire[:len(wire)-1],
		append([]byte("BAD1"), wire[4:]...),
	} {
		if _, _, err := hatSql.DecodeSQLRowBinaryWithStats(columns, corrupted); err == nil {
			t.Fatal("corrupted stats envelope unexpectedly decoded")
		}
	}
	stale := append([]byte(nil), wire...)
	metadataOffset := 4
	_, size := binary.Uvarint(stale[metadataOffset:])
	metadataOffset += size
	metadataLength, size := binary.Uvarint(stale[metadataOffset:])
	metadataOffset += size
	if metadataLength < 4 {
		t.Fatalf("unexpected metadata length %d", metadataLength)
	}
	_, size = binary.Uvarint(stale[metadataOffset:])
	metadataOffset += size
	_, size = binary.Uvarint(stale[metadataOffset:])
	metadataOffset += size
	metadataOffset++
	stale[metadataOffset] ^= 1
	if _, _, err := hatSql.DecodeSQLRowBinaryWithStats(columns, stale); err == nil {
		t.Fatal("stale column statistics unexpectedly accepted")
	}
}

func TestSQLRowBinaryStatsRejectsInvalidInput(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	for _, rows := range [][]hatSql.SQLRow{
		{{"id": "wrong"}},
		{{}},
	} {
		if _, err := hatSql.BuildSQLRowBinaryColumnStats(columns, rows); err == nil {
			t.Fatalf("invalid rows %#v unexpectedly produced stats", rows)
		}
	}
}

func statsTestColumns() []hatSql.SQLRowBinaryColumn {
	return []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "count", Type: hatSql.SQLRowBinaryUint64},
		{Name: "score", Type: hatSql.SQLRowBinaryFloat64},
		{Name: "active", Type: hatSql.SQLRowBinaryBool},
		{Name: "label", Type: hatSql.SQLRowBinaryString},
		{Name: "blob", Type: hatSql.SQLRowBinaryBytes},
		{Name: "day", Type: hatSql.SQLRowBinaryDate},
		{Name: "at", Type: hatSql.SQLRowBinaryDateTime},
		{Name: "ttl", Type: hatSql.SQLRowBinaryDuration},
		{Name: "uuid", Type: hatSql.SQLRowBinaryUUID},
		{Name: "metadata", Type: hatSql.SQLRowBinaryJSON},
		{Name: "optional", Type: hatSql.SQLRowBinaryString, Nullable: true},
	}
}

func BenchmarkSQLRowBinaryStatsEncode(b *testing.B) {
	columns := statsBenchmarkColumns()
	rows := statsBenchmarkRows()
	b.ReportAllocs()
	for range b.N {
		wire, err := hatSql.EncodeSQLRowBinaryWithStats(columns, rows)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(wire)), "wire-B")
	}
}

func BenchmarkSQLRowBinaryStatsBaselineEncode(b *testing.B) {
	columns := statsBenchmarkColumns()
	rows := statsBenchmarkRows()
	b.ReportAllocs()
	for range b.N {
		wire, err := hatSql.EncodeSQLRowBinary(columns, rows)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(wire)), "wire-B")
	}
}

func BenchmarkSQLRowBinaryStatsDecode(b *testing.B) {
	columns := statsBenchmarkColumns()
	rows := statsBenchmarkRows()
	wire, err := hatSql.EncodeSQLRowBinaryWithStats(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(wire)), "wire-B")
	for range b.N {
		if _, _, err := hatSql.DecodeSQLRowBinaryWithStats(columns, wire); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLRowBinaryStatsBaselineDecode(b *testing.B) {
	columns := statsBenchmarkColumns()
	rows := statsBenchmarkRows()
	wire, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(wire)), "wire-B")
	for range b.N {
		if _, err := hatSql.DecodeSQLRowBinary(columns, wire); err != nil {
			b.Fatal(err)
		}
	}
}

func statsBenchmarkColumns() []hatSql.SQLRowBinaryColumn {
	return []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "region", Type: hatSql.SQLRowBinaryString},
		{Name: "payload", Type: hatSql.SQLRowBinaryBytes},
		{Name: "metadata", Type: hatSql.SQLRowBinaryJSON},
	}
}

func statsBenchmarkRows() []hatSql.SQLRow {
	rows := make([]hatSql.SQLRow, 256)
	for index := range rows {
		rows[index] = hatSql.SQLRow{
			"id":       int64(index),
			"region":   []string{"sg", "jp", "us", "de"}[index%4],
			"payload":  []byte("repeated-payload"),
			"metadata": json.RawMessage(`{"tier":"warm"}`),
		}
	}
	return rows
}
