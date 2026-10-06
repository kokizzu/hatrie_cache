package hatDataStructure

import "testing"

var (
	tu20BaselineTuple VersionedTuple
	tu20BaselineWire  []byte
)

func BenchmarkTU20DirectVersionedTupleValidate(b *testing.B) {
	format, tuple := tu20BaselineVersionedTuple(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tuple.Validate(format); err != nil {
			b.Fatal(err)
		}
		tu20BaselineTuple = tuple
	}
}

func BenchmarkTU20DirectVersionedTupleMarshal(b *testing.B) {
	_, tuple := tu20BaselineVersionedTuple(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wire, err := MarshalVersionedTuple(tuple)
		if err != nil {
			b.Fatal(err)
		}
		tu20BaselineWire = wire
	}
}

func BenchmarkTU20DirectVersionedTupleUnmarshal(b *testing.B) {
	_, tuple := tu20BaselineVersionedTuple(b)
	wire, err := MarshalVersionedTuple(tuple)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded, err := UnmarshalVersionedTuple(wire)
		if err != nil {
			b.Fatal(err)
		}
		tu20BaselineTuple = decoded
	}
}

func tu20BaselineVersionedTuple(b *testing.B) (TupleFormat, VersionedTuple) {
	b.Helper()
	format, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "active", Type: TupleFieldBool},
	})
	if err != nil {
		b.Fatal(err)
	}
	tuple, err := format.PackVersioned([]TupleFieldValue{
		TupleInt64(42),
		TupleString("alice"),
		TupleBool(true),
	})
	if err != nil {
		b.Fatal(err)
	}
	return format, tuple
}
