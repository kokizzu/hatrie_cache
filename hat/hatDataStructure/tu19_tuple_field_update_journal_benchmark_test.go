package hatDataStructure

import (
	json "github.com/goccy/go-json"
	"testing"
)

type tupleFieldUpdateJSONJournalBaseline struct {
	Sequence uint64             `json:"sequence"`
	Key      string             `json:"key"`
	Updates  []TupleFieldUpdate `json:"updates"`
}

var tupleFieldUpdateJournalBenchmarkSink []byte
var tupleFieldUpdateJournalBenchmarkDecoded tupleFieldUpdateJSONJournalBaseline
var tupleFieldUpdateJournalBinaryBenchmarkDecoded TupleFieldUpdateJournalRecord

func benchmarkTupleFieldUpdateJournalUpdates() []TupleFieldUpdate {
	return []TupleFieldUpdate{
		{Index: 0, Kind: TupleFieldSet, Value: []byte("active")},
		{Index: 1, Kind: TupleFieldSplice, Start: 2, Remove: 1, Insert: []byte("XYZ")},
		{Index: 2, Kind: TupleFieldAddInt64, Delta: 7},
		{Index: 3, Kind: TupleFieldSet, Value: []byte("region-eu-west")},
	}
}

func benchmarkTupleFieldUpdateJournalRecord() TupleFieldUpdateJournalRecord {
	return TupleFieldUpdateJournalRecord{
		Sequence: 42,
		Key:      "customer-000042",
		Updates:  benchmarkTupleFieldUpdateJournalUpdates(),
	}
}

func BenchmarkTUG19TupleFieldUpdateJSONMarshalBaseline(b *testing.B) {
	record := tupleFieldUpdateJSONJournalBaseline{
		Sequence: 42,
		Key:      "customer-000042",
		Updates:  benchmarkTupleFieldUpdateJournalUpdates(),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := json.Marshal(record)
		if err != nil {
			b.Fatal(err)
		}
		tupleFieldUpdateJournalBenchmarkSink = payload
	}
	b.ReportMetric(float64(len(tupleFieldUpdateJournalBenchmarkSink)), "wire_bytes/op")
}

func BenchmarkTUG19TupleFieldUpdateJSONUnmarshalBaseline(b *testing.B) {
	record := tupleFieldUpdateJSONJournalBaseline{
		Sequence: 42,
		Key:      "customer-000042",
		Updates:  benchmarkTupleFieldUpdateJournalUpdates(),
	}
	payload, err := json.Marshal(record)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var decoded tupleFieldUpdateJSONJournalBaseline
		if err := json.Unmarshal(payload, &decoded); err != nil {
			b.Fatal(err)
		}
		tupleFieldUpdateJournalBenchmarkDecoded = decoded
	}
	b.ReportMetric(float64(len(payload)), "wire_bytes/op")
}

func BenchmarkTUG19TupleFieldUpdateBinaryMarshal(b *testing.B) {
	record := benchmarkTupleFieldUpdateJournalRecord()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := MarshalTupleFieldUpdateJournalRecord(record)
		if err != nil {
			b.Fatal(err)
		}
		tupleFieldUpdateJournalBenchmarkSink = payload
	}
	b.ReportMetric(float64(len(tupleFieldUpdateJournalBenchmarkSink)), "wire_bytes/op")
}

func BenchmarkTUG19TupleFieldUpdateBinaryUnmarshal(b *testing.B) {
	payload, err := MarshalTupleFieldUpdateJournalRecord(benchmarkTupleFieldUpdateJournalRecord())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decoded, err := UnmarshalTupleFieldUpdateJournalRecord(payload)
		if err != nil {
			b.Fatal(err)
		}
		tupleFieldUpdateJournalBinaryBenchmarkDecoded = decoded
	}
	b.ReportMetric(float64(len(payload)), "wire_bytes/op")
}
