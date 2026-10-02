package hatDataStructure

import (
	"encoding/json"
	"testing"
)

var tupleFieldUpdateJournalBenchmarkSink []byte

type tupleFieldUpdateJournalJSONUpdate struct {
	Index  int                  `json:"index"`
	Kind   TupleFieldUpdateKind `json:"kind"`
	Value  []byte               `json:"value,omitempty"`
	Start  int                  `json:"start,omitempty"`
	Remove int                  `json:"remove,omitempty"`
	Insert []byte               `json:"insert,omitempty"`
	Delta  int64                `json:"delta,omitempty"`
}

type tupleFieldUpdateJournalJSONRecord struct {
	Sequence      uint64                              `json:"sequence"`
	SchemaVersion uint64                              `json:"schema_version"`
	Updates       []tupleFieldUpdateJournalJSONUpdate `json:"updates"`
}

func benchmarkTupleFieldUpdateJournalRecord() TupleFieldUpdateJournalRecord {
	return TupleFieldUpdateJournalRecord{
		Sequence:      42,
		SchemaVersion: 7,
		Updates: []TupleFieldUpdate{
			{Index: 0, Kind: TupleFieldSet, Value: []byte("customer-000042")},
			{Index: 1, Kind: TupleFieldSplice, Start: 2, Remove: 3, Insert: []byte("region")},
			{Index: 2, Kind: TupleFieldAddInt64, Delta: 17},
		},
	}
}

func benchmarkTupleFieldUpdateJournalJSONRecord() tupleFieldUpdateJournalJSONRecord {
	return tupleFieldUpdateJournalJSONRecord{
		Sequence:      42,
		SchemaVersion: 7,
		Updates: []tupleFieldUpdateJournalJSONUpdate{
			{Index: 0, Kind: TupleFieldSet, Value: []byte("customer-000042")},
			{Index: 1, Kind: TupleFieldSplice, Start: 2, Remove: 3, Insert: []byte("region")},
			{Index: 2, Kind: TupleFieldAddInt64, Delta: 17},
		},
	}
}

func BenchmarkTupleFieldUpdateJournalCodec(b *testing.B) {
	record := benchmarkTupleFieldUpdateJournalRecord()
	jsonRecord := benchmarkTupleFieldUpdateJournalJSONRecord()
	b.Run("HTU1", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			encoded, err := MarshalTupleFieldUpdateJournalRecord(record)
			if err != nil {
				b.Fatal(err)
			}
			tupleFieldUpdateJournalBenchmarkSink = encoded
		}
		b.ReportMetric(float64(len(tupleFieldUpdateJournalBenchmarkSink)), "wire-bytes/op")
	})
	b.Run("JSON-baseline", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			encoded, err := json.Marshal(jsonRecord)
			if err != nil {
				b.Fatal(err)
			}
			tupleFieldUpdateJournalBenchmarkSink = encoded
		}
		b.ReportMetric(float64(len(tupleFieldUpdateJournalBenchmarkSink)), "wire-bytes/op")
	})
}

func BenchmarkTupleFieldUpdateJournalDecode(b *testing.B) {
	encoded, err := MarshalTupleFieldUpdateJournalRecord(benchmarkTupleFieldUpdateJournalRecord())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for range b.N {
		record, err := UnmarshalTupleFieldUpdateJournalRecord(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(record.Updates) != 3 {
			b.Fatal("decoded update count changed")
		}
	}
}

func BenchmarkTupleFieldUpdateJournalAppend(b *testing.B) {
	update := []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("customer-000042")}}
	b.Run("buffered", func(b *testing.B) {
		journal, err := OpenTupleFieldUpdateJournalWithOptions(b.TempDir()+"/updates.journal", TupleFieldUpdateJournalOptions{SyncOnAppend: false})
		if err != nil {
			b.Fatal(err)
		}
		defer journal.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := journal.Append(7, update); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sync-on-append", func(b *testing.B) {
		journal, err := OpenTupleFieldUpdateJournal(b.TempDir() + "/updates.journal")
		if err != nil {
			b.Fatal(err)
		}
		defer journal.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := journal.Append(7, update); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sync-on-append-batch-16", func(b *testing.B) {
		journal, err := OpenTupleFieldUpdateJournal(b.TempDir() + "/updates.journal")
		if err != nil {
			b.Fatal(err)
		}
		defer journal.Close()
		batch := make([]TupleFieldUpdateJournalAppend, 16)
		for index := range batch {
			batch[index] = TupleFieldUpdateJournalAppend{SchemaVersion: 7, Updates: update}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := journal.AppendBatch(batch); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(16, "records/op")
	})
}
