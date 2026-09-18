package hatDataStructure

import "testing"

var (
	tr020VersionedBenchmarkSink      VersionedTuple
	tr020VersionedBytesBenchmarkSink []byte
)

func BenchmarkTR020BaselineFormatValidate(b *testing.B) {
	format := mustTR020BenchmarkFormat(b, 17)
	tuple, err := format.Pack([]TupleFieldValue{TupleInt64(42), TupleString("orders"), TupleBytes([]byte("payload"))})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := format.Validate(tuple); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR020VersionedValidate(b *testing.B) {
	format := mustTR020BenchmarkFormat(b, 17)
	record, err := format.PackVersioned([]TupleFieldValue{TupleInt64(42), TupleString("orders"), TupleBytes([]byte("payload"))})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := record.Validate(format); err != nil {
			b.Fatal(err)
		}
	}
	tr020VersionedBenchmarkSink = record
}

func BenchmarkTR020VersionedMarshal(b *testing.B) {
	format := mustTR020BenchmarkFormat(b, 17)
	record, err := format.PackVersioned([]TupleFieldValue{TupleInt64(42), TupleString("orders"), TupleBytes([]byte("payload"))})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := MarshalVersionedTuple(record)
		if err != nil {
			b.Fatal(err)
		}
		tr020VersionedBytesBenchmarkSink = encoded
	}
}

func BenchmarkTR020VersionedUnmarshal(b *testing.B) {
	format := mustTR020BenchmarkFormat(b, 17)
	record, err := format.PackVersioned([]TupleFieldValue{TupleInt64(42), TupleString("orders"), TupleBytes([]byte("payload"))})
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := MarshalVersionedTuple(record)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decoded, err := UnmarshalVersionedTuple(encoded)
		if err != nil {
			b.Fatal(err)
		}
		tr020VersionedBenchmarkSink = decoded
	}
}

func mustTR020BenchmarkFormat(b testing.TB, version uint64) TupleFormat {
	b.Helper()
	format, err := NewTupleFormat(version, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "payload", Type: TupleFieldBytes},
	})
	if err != nil {
		b.Fatal(err)
	}
	return format
}
