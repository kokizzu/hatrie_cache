package hatDataStructure

import (
	"bytes"
	"io"
	"testing"
)

func benchmarkTU19Record() TupleFieldUpdateJournalRecord {
	return TupleFieldUpdateJournalRecord{
		Sequence:      1,
		TupleID:       99,
		SchemaVersion: 7,
		Updates: []TupleFieldUpdate{
			{Index: 0, Kind: TupleFieldSet, Value: []byte("east")},
			{Index: 1, Kind: TupleFieldAddInt64, Delta: 1},
			{Index: 2, Kind: TupleFieldSplice, Start: 1, Remove: 2, Insert: []byte("XY")},
		},
	}
}

func BenchmarkTU19TupleUpdateJournalAppend(b *testing.B) {
	journal, err := NewTupleFieldUpdateJournal(io.Discard, TupleFieldUpdateJournalOptions{NoSync: true})
	if err != nil {
		b.Fatal(err)
	}
	updates := benchmarkTU19Record().Updates
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := journal.Append(99, 7, updates); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU19TupleUpdateJournalMarshal(b *testing.B) {
	record := benchmarkTU19Record()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		record.Sequence = uint64(index + 1)
		if _, err := MarshalTupleFieldUpdateJournalRecord(record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU19TupleUpdateJournalReplay(b *testing.B) {
	var data bytes.Buffer
	journal, err := NewTupleFieldUpdateJournal(&data, TupleFieldUpdateJournalOptions{NoSync: true})
	if err != nil {
		b.Fatal(err)
	}
	updates := benchmarkTU19Record().Updates
	for index := 0; index < 100; index++ {
		if _, err := journal.Append(uint64(index), 7, updates); err != nil {
			b.Fatal(err)
		}
	}
	payload := append([]byte(nil), data.Bytes()...)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var applied uint64
		last, err := ReplayTupleFieldUpdateJournal(bytes.NewReader(payload), 0, TupleFieldUpdateJournalReplayOptions{}, func(record TupleFieldUpdateJournalRecord) error {
			applied += record.TupleID + uint64(len(record.Updates))
			return nil
		})
		if err != nil || last != 100 || applied == 0 {
			b.Fatalf("ReplayTupleFieldUpdateJournal() = last %d applied %d err %v", last, applied, err)
		}
	}
}
