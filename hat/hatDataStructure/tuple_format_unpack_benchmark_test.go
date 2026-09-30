package hatDataStructure

import (
	"testing"
	"time"
)

var tupleFormatUnpackSink []TupleFieldValue

func BenchmarkTupleFormatUnpack(b *testing.B) {
	format, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "name", Type: TupleFieldString},
		{Name: "payload", Type: TupleFieldBytes},
		{Name: "count", Type: TupleFieldInt64},
		{Name: "active", Type: TupleFieldBool},
		{Name: "at", Type: TupleFieldTimestamp},
	})
	if err != nil {
		b.Fatal(err)
	}
	tuple, err := format.Pack([]TupleFieldValue{
		TupleString("alpha"),
		TupleBytes([]byte{1, 2, 3, 4, 5, 6, 7, 8}),
		TupleInt64(7),
		TupleBool(true),
		TupleTimestamp(time.Unix(123, 456).UTC()),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.Run("unpack", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			values, err := format.Unpack(tuple)
			if err != nil {
				b.Fatal(err)
			}
			tupleFormatUnpackSink = values
		}
	})
	b.Run("unpack_into", func(b *testing.B) {
		destination := make([]TupleFieldValue, 0, format.FieldCount())
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			values, err := format.UnpackInto(tuple, destination)
			if err != nil {
				b.Fatal(err)
			}
			destination = values
			tupleFormatUnpackIntoSink = values
		}
	})
}

var tupleFormatUnpackIntoSink []TupleFieldValue
