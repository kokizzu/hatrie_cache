package hatSql

import (
	"bytes"
	"testing"
)

var mz049QuarantineSink Row

func BenchmarkMZ049SchemaDriftQuarantine(b *testing.B) {
	rows := mz049BenchmarkRows()
	columns := []SQLRowBinaryColumn{
		{Name: "active", Type: SQLRowBinaryBool},
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "metadata", Type: SQLRowBinaryJSON},
		{Name: "name", Type: SQLRowBinaryString},
		{Name: "score", Type: SQLRowBinaryFloat64},
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_, err := QuarantineExternalRows(rows, columns, ExternalSchemaQuarantineOptions{}, func(row Row) error {
			mz049QuarantineSink = row
			return nil
		}, func(ExternalSchemaQuarantineRecord) error {
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ049SchemaDriftJSONStreamQuarantine(b *testing.B) {
	data := mz049BenchmarkNDJSONData()
	columns := []SQLRowBinaryColumn{
		{Name: "active", Type: SQLRowBinaryBool},
		{Name: "id", Type: SQLRowBinaryFloat64},
		{Name: "metadata", Type: SQLRowBinaryJSON},
		{Name: "name", Type: SQLRowBinaryString},
		{Name: "score", Type: SQLRowBinaryFloat64},
	}
	options := ExternalSchemaQuarantineOptions{AllowNumericPromotion: true}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_, err := QuarantineExternalJSONEachRow(bytes.NewReader(data), ExternalImportOptions{}, columns, options, func(row Row) error {
			mz049QuarantineSink = row
			return nil
		}, func(ExternalSchemaQuarantineRecord) error {
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
