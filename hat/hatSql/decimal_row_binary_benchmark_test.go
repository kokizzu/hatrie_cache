package hatSql

import (
	"fmt"
	"testing"
)

var sqlRowBinaryDecimalBenchmarkEncoded []byte
var sqlRowBinaryDecimalBenchmarkRows []SQLRow

func sqlRowBinaryDecimalBenchmarkStringFixture() ([]SQLRowBinaryColumn, []SQLRow) {
	columns := []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryString}}
	rows := make([]SQLRow, 10_000)
	for index := range rows {
		rows[index] = SQLRow{"amount": "12345678901234567890." + decimalBenchmarkFraction(index)}
	}
	return columns, rows
}

func decimalBenchmarkFraction(index int) string {
	value := index % 10_000
	return string([]byte{
		byte('0' + value/1000),
		byte('0' + (value/100)%10),
		byte('0' + (value/10)%10),
		byte('0' + value%10),
	})
}

func BenchmarkSQLRowBinaryDecimalStringBaselineEncode(b *testing.B) {
	columns, rows := sqlRowBinaryDecimalBenchmarkStringFixture()
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
	sqlRowBinaryDecimalBenchmarkEncoded = encoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func BenchmarkSQLRowBinaryDecimalStringBaselineDecode(b *testing.B) {
	columns, rows := sqlRowBinaryDecimalBenchmarkStringFixture()
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
	sqlRowBinaryDecimalBenchmarkRows = decoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func sqlRowBinaryDecimalBenchmark128Fixture() ([]SQLRowBinaryColumn, []SQLRow) {
	columns := []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal128, DecimalScale: 4, DecimalPrecision: 38}}
	rows := make([]SQLRow, 10000)
	for index := range rows {
		value, err := ParseSQLDecimal128(fmt.Sprintf("12345678901234567890.%04d", index), 4)
		if err != nil {
			panic(err)
		}
		rows[index] = SQLRow{"amount": value}
	}
	return columns, rows
}

func sqlRowBinaryDecimalBenchmark256Fixture() ([]SQLRowBinaryColumn, []SQLRow) {
	columns := []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal256, DecimalScale: 4, DecimalPrecision: 76}}
	rows := make([]SQLRow, 10000)
	for index := range rows {
		value, err := ParseSQLDecimal256(fmt.Sprintf("12345678901234567890.%04d", index), 4)
		if err != nil {
			panic(err)
		}
		rows[index] = SQLRow{"amount": value}
	}
	return columns, rows
}

func BenchmarkSQLRowBinaryDecimal128TypedEncode(b *testing.B) {
	columns, rows := sqlRowBinaryDecimalBenchmark128Fixture()
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
	sqlRowBinaryDecimalBenchmarkEncoded = encoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func BenchmarkSQLRowBinaryDecimal128TypedDecode(b *testing.B) {
	columns, rows := sqlRowBinaryDecimalBenchmark128Fixture()
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
	sqlRowBinaryDecimalBenchmarkRows = decoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func BenchmarkSQLRowBinaryDecimal256TypedEncode(b *testing.B) {
	columns, rows := sqlRowBinaryDecimalBenchmark256Fixture()
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
	sqlRowBinaryDecimalBenchmarkEncoded = encoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}

func BenchmarkSQLRowBinaryDecimal256TypedDecode(b *testing.B) {
	columns, rows := sqlRowBinaryDecimalBenchmark256Fixture()
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
	sqlRowBinaryDecimalBenchmarkRows = decoded
	b.ReportMetric(float64(len(encoded)), "payload-bytes")
}
