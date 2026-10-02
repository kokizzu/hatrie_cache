package hatDataStructure

import (
	"encoding/json"
	"testing"
)

var (
	tupleFieldUpdateJournalBenchmarkBinary []byte
	tupleFieldUpdateJournalBenchmarkJSON   []byte
	tupleFieldUpdateJournalBenchmarkRecord TupleFieldUpdateJournal
)

func BenchmarkTupleFieldUpdateJournalMarshal(b *testing.B) {
	record := tupleFieldUpdateJournalBenchmarkRecordValue(b)
	encoded, err := record.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	b.ReportMetric(float64(len(encoded)), "wire-bytes")
	for index := 0; index < b.N; index++ {
		encoded, encodeErr := record.MarshalBinary()
		if encodeErr != nil {
			b.Fatal(encodeErr)
		}
		tupleFieldUpdateJournalBenchmarkBinary = encoded
	}
}

func BenchmarkJSONTupleFieldUpdateJournalMarshal(b *testing.B) {
	record := tupleFieldUpdateJournalBenchmarkRecordValue(b)
	encoded, err := json.Marshal(record)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	b.ReportMetric(float64(len(encoded)), "wire-bytes")
	for index := 0; index < b.N; index++ {
		encoded, encodeErr := json.Marshal(record)
		if encodeErr != nil {
			b.Fatal(encodeErr)
		}
		tupleFieldUpdateJournalBenchmarkJSON = encoded
	}
}

func BenchmarkTupleFieldUpdateJournalUnmarshal(b *testing.B) {
	record := tupleFieldUpdateJournalBenchmarkRecordValue(b)
	encoded, err := record.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	b.ReportMetric(float64(len(encoded)), "wire-bytes")
	for index := 0; index < b.N; index++ {
		decoded, decodeErr := UnmarshalTupleFieldUpdateJournal(encoded)
		if decodeErr != nil {
			b.Fatal(decodeErr)
		}
		tupleFieldUpdateJournalBenchmarkRecord = decoded
	}
}

func BenchmarkJSONTupleFieldUpdateJournalUnmarshal(b *testing.B) {
	record := tupleFieldUpdateJournalBenchmarkRecordValue(b)
	encoded, err := json.Marshal(record)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	b.ReportMetric(float64(len(encoded)), "wire-bytes")
	for index := 0; index < b.N; index++ {
		var decoded TupleFieldUpdateJournal
		if decodeErr := json.Unmarshal(encoded, &decoded); decodeErr != nil {
			b.Fatal(decodeErr)
		}
		tupleFieldUpdateJournalBenchmarkRecord = decoded
	}
}

func tupleFieldUpdateJournalBenchmarkRecordValue(b *testing.B) TupleFieldUpdateJournal {
	b.Helper()
	return TupleFieldUpdateJournal{
		Sequence: 99,
		Updates: []TupleFieldUpdate{
			{Index: 0, Kind: TupleFieldSet, Value: []byte("east")},
			{Index: 1, Kind: TupleFieldAddInt64, Delta: 8},
			{Index: 2, Kind: TupleFieldSplice, Start: 2, Remove: 2, Insert: []byte("XYZ")},
		},
	}
}
