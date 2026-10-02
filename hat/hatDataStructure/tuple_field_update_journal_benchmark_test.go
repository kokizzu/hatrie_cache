package hatDataStructure

import "testing"

var tupleFieldUpdateJournalBenchmarkCacheSink TupleFieldOffsetCache
var tupleFieldUpdateJournalBenchmarkTupleSink VersionedTuple

func benchmarkTupleFieldUpdateJournalFixture(b *testing.B) (VersionedTuple, TupleFormat, []TupleFieldUpdate) {
	b.Helper()
	format, err := NewTupleFormat(7, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "payload", Type: TupleFieldBytes},
	})
	if err != nil {
		b.Fatal(err)
	}
	tuple, err := NewVersionedTuple(format, []TupleFieldValue{
		TupleInt64(10), TupleString("alice"), TupleBytes([]byte{1, 2, 3}),
	})
	if err != nil {
		b.Fatal(err)
	}
	return tuple, format, []TupleFieldUpdate{
		{Index: 0, Kind: TupleFieldAddInt64, Delta: 5},
		{Index: 1, Kind: TupleFieldSplice, Start: 5, Insert: []byte("-admin")},
	}
}

func BenchmarkTupleFieldUpdateJournalDirect(b *testing.B) {
	tuple, _, updates := benchmarkTupleFieldUpdateJournalFixture(b)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		updated, err := tuple.Tuple().ApplyUpdates(updates)
		if err != nil {
			b.Fatal(err)
		}
		tupleFieldUpdateJournalBenchmarkCacheSink = updated
	}
}

func BenchmarkTupleFieldUpdateJournalRoundTrip(b *testing.B) {
	tuple, format, updates := benchmarkTupleFieldUpdateJournalFixture(b)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		record := TupleFieldUpdateJournalRecord{
			Sequence:      uint64(index + 1),
			SchemaVersion: format.Version(),
			Key:           []byte("account:42"),
			Updates:       updates,
		}
		wire, err := MarshalTupleFieldUpdateJournal(record)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := UnmarshalTupleFieldUpdateJournal(wire)
		if err != nil {
			b.Fatal(err)
		}
		updated, err := tuple.ApplyTupleFieldUpdateJournal(format, decoded)
		if err != nil {
			b.Fatal(err)
		}
		tupleFieldUpdateJournalBenchmarkTupleSink = updated
		b.SetBytes(int64(len(wire)))
	}
}
