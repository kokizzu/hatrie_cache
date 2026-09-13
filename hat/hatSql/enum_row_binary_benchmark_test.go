package hatSql

import "testing"

var sqlRowBinaryEnumBenchmarkEncoded []byte
var sqlRowBinaryEnumBenchmarkRows []SQLRow

func sqlRowBinaryEnumBenchmarkStringFixture() ([]SQLRowBinaryColumn, []SQLRow) {
	columns := []SQLRowBinaryColumn{{Name: "status", Type: SQLRowBinaryString}}
	labels := [...]string{"queued", "running", "done", "failed"}
	rows := make([]SQLRow, 10_000)
	for index := range rows {
		rows[index] = SQLRow{"status": labels[index%len(labels)]}
	}
	return columns, rows
}

func sqlRowBinaryEnumBenchmarkTypedFixture() ([]SQLRowBinaryColumn, []SQLRow) {
	columns := []SQLRowBinaryColumn{
		{Name: "status", Type: SQLRowBinaryEnum8, EnumValues: []string{"queued", "running", "done", "failed"}},
	}
	rows := make([]SQLRow, 10_000)
	for index := range rows {
		rows[index] = SQLRow{"status": SQLEnum8(index % 4)}
	}
	return columns, rows
}

func BenchmarkSQLRowBinaryEnumStringBaselineEncode(b *testing.B) {
	columns, rows := sqlRowBinaryEnumBenchmarkStringFixture()
	b.ReportAllocs()
	b.ResetTimer()
	var encoded []byte
	for range b.N {
		var err error
		encoded, err = EncodeSQLRowBinary(columns, rows)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	sqlRowBinaryEnumBenchmarkEncoded = encoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func BenchmarkSQLRowBinaryEnumStringBaselineDecode(b *testing.B) {
	columns, rows := sqlRowBinaryEnumBenchmarkStringFixture()
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var decoded []SQLRow
	for range b.N {
		decoded, err = DecodeSQLRowBinary(columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	sqlRowBinaryEnumBenchmarkRows = decoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func BenchmarkSQLRowBinaryEnumTypedEncode(b *testing.B) {
	columns, rows := sqlRowBinaryEnumBenchmarkTypedFixture()
	b.ReportAllocs()
	b.ResetTimer()
	var encoded []byte
	for range b.N {
		var err error
		encoded, err = EncodeSQLRowBinary(columns, rows)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	sqlRowBinaryEnumBenchmarkEncoded = encoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func BenchmarkSQLRowBinaryEnumTypedDecode(b *testing.B) {
	columns, rows := sqlRowBinaryEnumBenchmarkTypedFixture()
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var decoded []SQLRow
	for range b.N {
		decoded, err = DecodeSQLRowBinary(columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	sqlRowBinaryEnumBenchmarkRows = decoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func BenchmarkSQLRowBinaryEnumStringBaselineSerialDecode(b *testing.B) {
	columns, rows := sqlRowBinaryEnumBenchmarkStringFixture()
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var decoded []SQLRow
	for range b.N {
		decoded, err = decodeSQLRowBinarySerial(columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	sqlRowBinaryEnumBenchmarkRows = decoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func BenchmarkSQLRowBinaryEnumTypedSerialDecode(b *testing.B) {
	columns, rows := sqlRowBinaryEnumBenchmarkTypedFixture()
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var decoded []SQLRow
	for range b.N {
		decoded, err = decodeSQLRowBinarySerial(columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	sqlRowBinaryEnumBenchmarkRows = decoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func BenchmarkSQLRowBinaryEnumStringBaselineParallelDecode(b *testing.B) {
	columns, rows := sqlRowBinaryEnumBenchmarkStringFixture()
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var decoded []SQLRow
	for range b.N {
		decoded, err = DecodeSQLRowBinaryParallel(columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	sqlRowBinaryEnumBenchmarkRows = decoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func BenchmarkSQLRowBinaryEnumTypedParallelDecode(b *testing.B) {
	columns, rows := sqlRowBinaryEnumBenchmarkTypedFixture()
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var decoded []SQLRow
	for range b.N {
		decoded, err = DecodeSQLRowBinaryParallel(columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	sqlRowBinaryEnumBenchmarkRows = decoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}
