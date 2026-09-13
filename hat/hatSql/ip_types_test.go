package hatSql

import (
	"bytes"
	"reflect"
	"testing"
)

func TestSQLIPTypesParseFormatAndOrder(t *testing.T) {
	v4, err := ParseSQLIPv4("192.0.2.1")
	if err != nil {
		t.Fatalf("ParseSQLIPv4() error = %v", err)
	}
	if got := v4.String(); got != "192.0.2.1" {
		t.Fatalf("IPv4 String() = %q", got)
	}
	nextV4, err := ParseSQLIPv4("192.0.2.2")
	if err != nil {
		t.Fatalf("ParseSQLIPv4() next error = %v", err)
	}
	if v4 >= nextV4 {
		t.Fatalf("IPv4 ordering is not numeric: %v >= %v", v4, nextV4)
	}

	v6, err := ParseSQLIPv6("2001:0DB8:0:0:0:0:0:1")
	if err != nil {
		t.Fatalf("ParseSQLIPv6() error = %v", err)
	}
	if got := v6.String(); got != "2001:db8::1" {
		t.Fatalf("IPv6 String() = %q", got)
	}

	for _, invalid := range []string{"", "2001:db8::1", "999.0.0.1", "192.0.2.1/32"} {
		if _, err := ParseSQLIPv4(invalid); err == nil {
			t.Errorf("ParseSQLIPv4(%q) error = nil", invalid)
		}
	}
	for _, invalid := range []string{"", "192.0.2.1", "2001:db8::1/128", "2001:::1"} {
		if _, err := ParseSQLIPv6(invalid); err == nil {
			t.Errorf("ParseSQLIPv6(%q) error = nil", invalid)
		}
	}
}

func TestSQLRowBinaryIPRoundTrip(t *testing.T) {
	v4a, err := ParseSQLIPv4("192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	v4b, err := ParseSQLIPv4("198.51.100.10")
	if err != nil {
		t.Fatal(err)
	}
	v6a, err := ParseSQLIPv6("2001:db8::1")
	if err != nil {
		t.Fatal(err)
	}
	v6b, err := ParseSQLIPv6("fd00::42")
	if err != nil {
		t.Fatal(err)
	}
	columns := []SQLRowBinaryColumn{
		{Name: "v4", Type: SQLRowBinaryIPv4},
		{Name: "v6", Type: SQLRowBinaryIPv6},
		{Name: "nullable_v4", Type: SQLRowBinaryIPv4, Nullable: true},
	}
	rows := []SQLRow{
		{"v4": v4a, "v6": v6a, "nullable_v4": v4b},
		{"v4": v4b, "v6": v6b, "nullable_v4": nil},
	}
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinary() error = %v", err)
	}
	if got, want := len(encoded), 4+16+1+4+16+1+4; got != want {
		t.Fatalf("encoded length = %d, want %d", got, want)
	}
	decoded, err := DecodeSQLRowBinary(columns, encoded)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinary() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatalf("decoded rows = %#v, want %#v", decoded, rows)
	}
}

func TestSQLRowBinaryIPBitmapAndStats(t *testing.T) {
	v4a, _ := ParseSQLIPv4("192.0.2.1")
	v4b, _ := ParseSQLIPv4("192.0.2.2")
	v6a, _ := ParseSQLIPv6("2001:db8::1")
	v6b, _ := ParseSQLIPv6("2001:db8::2")
	columns := []SQLRowBinaryColumn{
		{Name: "v4", Type: SQLRowBinaryIPv4, Nullable: true},
		{Name: "v6", Type: SQLRowBinaryIPv6, Nullable: true},
	}
	rows := []SQLRow{{"v4": v4a, "v6": v6b}, {"v4": nil, "v6": v6a}, {"v4": v4b, "v6": nil}}
	encoded, err := EncodeSQLRowBinaryBitmap(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryBitmap() error = %v", err)
	}
	decoded, err := DecodeSQLRowBinaryBitmap(columns, encoded)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryBitmap() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatalf("bitmap decoded rows = %#v, want %#v", decoded, rows)
	}
	stats, err := BuildSQLRowBinaryColumnStats(columns, rows)
	if err != nil {
		t.Fatalf("BuildSQLRowBinaryColumnStats() error = %v", err)
	}
	if stats[0].Min != v4a || stats[0].Max != v4b || stats[1].Min != v6a || stats[1].Max != v6b {
		t.Fatalf("IP stats = %#v, want v4 %v..%v and v6 %v..%v", stats, v4a, v4b, v6a, v6b)
	}
}

func TestSQLRowBinaryIPStreamRoundTrip(t *testing.T) {
	v4, _ := ParseSQLIPv4("192.0.2.1")
	v6, _ := ParseSQLIPv6("2001:db8::1")
	rows := []Row{{"v4": v4, "v6": v6}, {"v4": v4, "v6": v6}}
	var encoded bytes.Buffer
	writer := NewSQLRowBinaryStreamWriter(&encoded, []string{"v4", "v6"})
	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatalf("WriteRow() error = %v", err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	columns, decoded, err := DecodeSQLRowBinaryStream(encoded.Bytes())
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryStream() error = %v", err)
	}
	wantColumns := []SQLRowBinaryColumn{
		{Name: "v4", Type: SQLRowBinaryIPv4, Nullable: true},
		{Name: "v6", Type: SQLRowBinaryIPv6, Nullable: true},
	}
	if !reflect.DeepEqual(columns, wantColumns) {
		t.Fatalf("stream columns = %#v, want %#v", columns, wantColumns)
	}
	if !reflect.DeepEqual(decoded, []Row{{"v4": v4, "v6": v6}, {"v4": v4, "v6": v6}}) {
		t.Fatalf("stream rows = %#v", decoded)
	}
}

func BenchmarkSQLRowBinaryIPTyped(b *testing.B) {
	v4a, _ := ParseSQLIPv4("192.0.2.1")
	v4b, _ := ParseSQLIPv4("198.51.100.10")
	v4c, _ := ParseSQLIPv4("203.0.113.200")
	v4d, _ := ParseSQLIPv4("10.0.0.42")
	v6a, _ := ParseSQLIPv6("2001:db8::1")
	v6b, _ := ParseSQLIPv6("2001:db8:1::10")
	v6c, _ := ParseSQLIPv6("2001:db8:ffff::200")
	v6d, _ := ParseSQLIPv6("fd00::42")
	columns := []SQLRowBinaryColumn{
		{Name: "v4", Type: SQLRowBinaryIPv4},
		{Name: "v6", Type: SQLRowBinaryIPv6},
	}
	rows := []SQLRow{
		{"v4": v4a, "v6": v6a},
		{"v4": v4b, "v6": v6b},
		{"v4": v4c, "v6": v6c},
		{"v4": v4d, "v6": v6d},
	}
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err = EncodeSQLRowBinary(columns, rows)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := DecodeSQLRowBinary(columns, encoded)
		if err != nil || len(decoded) != len(rows) {
			b.Fatalf("round trip failed: rows=%d err=%v", len(decoded), err)
		}
	}
}
