package hatDataStructure

import "testing"

const tupleFormatFieldsBenchmarkSize = 64

var tupleFormatFieldsSink []TupleFieldSpec

func BenchmarkTupleFormatFields(b *testing.B) {
	format := tupleFormatFieldsFixture(b)
	b.ReportAllocs()
	b.Run("fields", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			tupleFormatFieldsSink = format.Fields()
		}
	})
	b.Run("fields_into", func(b *testing.B) {
		destination := make([]TupleFieldSpec, 0, format.FieldCount())
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			destination = format.FieldsInto(destination)
			tupleFormatFieldsIntoSink = destination
		}
	})
}

var tupleFormatFieldsIntoSink []TupleFieldSpec

func tupleFormatFieldsFixture(b *testing.B) TupleFormat {
	fields := make([]TupleFieldSpec, tupleFormatFieldsBenchmarkSize)
	for index := range fields {
		fields[index] = TupleFieldSpec{Name: "field", Type: TupleFieldInt64}
		fields[index].Name += string(rune('a'+index%26)) + string(rune('0'+index/26))
	}
	format, err := NewTupleFormat(1, fields)
	if err != nil {
		b.Fatal(err)
	}
	return format
}
