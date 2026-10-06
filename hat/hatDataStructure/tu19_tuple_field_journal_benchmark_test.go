package hatDataStructure

import "testing"

var (
	tu19JournalBenchmarkBytes []byte
	tu19JournalBenchmarkCache TupleFieldOffsetCache
)

func BenchmarkTU19JournalEncode(b *testing.B) {
	record := tu19JournalBenchmarkRecord()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded, err := EncodeTupleFieldUpdateJournalRecord(record)
		if err != nil {
			b.Fatal(err)
		}
		tu19JournalBenchmarkBytes = encoded
	}
}

func BenchmarkTU19JournalDecode(b *testing.B) {
	encoded, err := EncodeTupleFieldUpdateJournalRecord(tu19JournalBenchmarkRecord())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, err := DecodeTupleFieldUpdateJournalRecord(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(record.Updates) == 0 {
			b.Fatal("decoded journal has no updates")
		}
	}
}

func BenchmarkTU19JournalReplay(b *testing.B) {
	cache, err := NewPackedTuple([][]byte{[]byte("key"), make([]byte, 8)})
	if err != nil {
		b.Fatal(err)
	}
	record := TupleFieldUpdateJournalRecord{
		Sequence:      1,
		FormatVersion: 1,
		Updates: []TupleFieldUpdate{{
			Index: 1,
			Kind:  TupleFieldAddInt64,
			Delta: 1,
		}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache, err = ApplyTupleFieldUpdateJournalRecord(cache, record, 1)
		if err != nil {
			b.Fatal(err)
		}
	}
	tu19JournalBenchmarkCache = cache
}

func tu19JournalBenchmarkRecord() TupleFieldUpdateJournalRecord {
	return TupleFieldUpdateJournalRecord{
		Sequence:      1,
		FormatVersion: 1,
		Updates: []TupleFieldUpdate{
			{Index: 0, Kind: TupleFieldSet, Value: []byte("after")},
			{Index: 1, Kind: TupleFieldSplice, Start: 1, Remove: 1, Insert: []byte("XYZ")},
			{Index: 2, Kind: TupleFieldAddInt64, Delta: 2},
		},
	}
}
