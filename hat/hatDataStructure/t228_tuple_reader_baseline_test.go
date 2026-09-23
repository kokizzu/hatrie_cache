package hatDataStructure

import "testing"

func BenchmarkT228TupleFormatUnpackBaseline(b *testing.B) {
	format, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldUint64},
		{Name: "name", Type: TupleFieldString},
		{Name: "region", Type: TupleFieldString},
		{Name: "active", Type: TupleFieldBool},
	})
	if err != nil {
		b.Fatal(err)
	}
	tuple, err := format.Pack([]TupleFieldValue{
		TupleUint64(42),
		TupleString("orders"),
		TupleString("sg"),
		TupleBool(true),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, err := format.Unpack(tuple)
		if err != nil {
			b.Fatal(err)
		}
		t228TupleReaderBenchmarkSink = values
	}
}

var t228TupleReaderBenchmarkSink []TupleFieldValue
