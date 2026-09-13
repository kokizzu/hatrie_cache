package hatSql

import "testing"

var sqlRowBinaryIPStringBaselineColumns = []SQLRowBinaryColumn{
	{Name: "v4", Type: SQLRowBinaryString},
	{Name: "v6", Type: SQLRowBinaryString},
}

var sqlRowBinaryIPStringBaselineRows = []SQLRow{
	{"v4": "192.0.2.1", "v6": "2001:db8::1"},
	{"v4": "198.51.100.10", "v6": "2001:db8:1::10"},
	{"v4": "203.0.113.200", "v6": "2001:db8:ffff::200"},
	{"v4": "10.0.0.42", "v6": "fd00::42"},
}

func BenchmarkSQLRowBinaryIPStringBaseline(b *testing.B) {
	encoded, err := EncodeSQLRowBinary(sqlRowBinaryIPStringBaselineColumns, sqlRowBinaryIPStringBaselineRows)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err = EncodeSQLRowBinary(sqlRowBinaryIPStringBaselineColumns, sqlRowBinaryIPStringBaselineRows)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := DecodeSQLRowBinary(sqlRowBinaryIPStringBaselineColumns, encoded)
		if err != nil || len(decoded) != len(sqlRowBinaryIPStringBaselineRows) {
			b.Fatalf("round trip failed: rows=%d err=%v", len(decoded), err)
		}
	}
}
