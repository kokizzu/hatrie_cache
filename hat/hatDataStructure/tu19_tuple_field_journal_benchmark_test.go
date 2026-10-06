package hatDataStructure

import "testing"

func benchmarkTU19VersionedTuple(b *testing.B) VersionedTuple {
	b.Helper()
	format, err := NewTupleFormat(17, []TupleFieldSpec{
		{Name: "count", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "payload", Type: TupleFieldBytes},
	})
	if err != nil {
		b.Fatalf("NewTupleFormat() error = %v", err)
	}
	tuple, err := format.PackVersioned([]TupleFieldValue{
		TupleInt64(10),
		TupleString("before"),
		TupleBytes([]byte("abcd")),
	})
	if err != nil {
		b.Fatalf("PackVersioned() error = %v", err)
	}
	return tuple
}

func benchmarkTU19Updates() []TupleFieldUpdate {
	return []TupleFieldUpdate{
		{Index: 0, Kind: TupleFieldAddInt64, Delta: 5},
		{Index: 1, Kind: TupleFieldSet, Value: []byte("after")},
		{Index: 2, Kind: TupleFieldSplice, Start: 1, Remove: 2, Value: []byte("XY")},
	}
}

func BenchmarkTU19VersionedTupleApplyUpdatesWithFormat(b *testing.B) {
	tuple := benchmarkTU19VersionedTuple(b)
	format, err := NewTupleFormat(17, []TupleFieldSpec{
		{Name: "count", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "payload", Type: TupleFieldBytes},
	})
	if err != nil {
		b.Fatalf("NewTupleFormat() error = %v", err)
	}
	updates := benchmarkTU19Updates()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tuple.ApplyUpdates(format, updates); err != nil {
			b.Fatalf("ApplyUpdates() error = %v", err)
		}
	}
}

func BenchmarkTU19VersionedTupleApplyFieldUpdates(b *testing.B) {
	tuple := benchmarkTU19VersionedTuple(b)
	updates := benchmarkTU19Updates()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tuple.ApplyFieldUpdates(updates); err != nil {
			b.Fatalf("ApplyFieldUpdates() error = %v", err)
		}
	}
}
