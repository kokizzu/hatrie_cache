package hatDataStructure

import (
	"encoding/json"
	"testing"
)

type tu19CompareJSONRecord struct {
	Sequence uint64             `json:"sequence"`
	Key      string             `json:"key"`
	Updates  []TupleFieldUpdate `json:"updates"`
}

var tu19CompareRecord = tu19CompareJSONRecord{
	Sequence: 42,
	Key:      "orders/42",
	Updates: []TupleFieldUpdate{
		{Index: 0, Kind: TupleFieldSet, Value: []byte("order-42")},
		{Index: 1, Kind: TupleFieldAddInt64, Delta: 10},
		{Index: 2, Kind: TupleFieldSplice, Start: 1, Remove: 2, Insert: []byte("XX")},
	},
}

var tu19CompareJSONPayload = mustMarshalTU19CompareJSON(tu19CompareRecord)
var tu19CompareBinaryPayload = mustMarshalTU19CompareBinary()

func mustMarshalTU19CompareJSON(record tu19CompareJSONRecord) []byte {
	payload, err := json.Marshal(record)
	if err != nil {
		panic(err)
	}
	return payload
}

func mustMarshalTU19CompareBinary() []byte {
	record := TupleFieldUpdateJournalRecord{
		Sequence: tu19CompareRecord.Sequence,
		Key:      tu19CompareRecord.Key,
		Updates:  tu19CompareRecord.Updates,
	}
	payload, err := MarshalTupleFieldUpdateJournalRecord(record)
	if err != nil {
		panic(err)
	}
	return payload
}

func BenchmarkTU19CompareJSONEncode(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(tu19CompareRecord); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU19CompareJSONDecode(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var record tu19CompareJSONRecord
		if err := json.Unmarshal(tu19CompareJSONPayload, &record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU19CompareBinaryEncode(b *testing.B) {
	b.ReportAllocs()
	record := TupleFieldUpdateJournalRecord{
		Sequence: tu19CompareRecord.Sequence,
		Key:      tu19CompareRecord.Key,
		Updates:  tu19CompareRecord.Updates,
	}
	for i := 0; i < b.N; i++ {
		if _, err := MarshalTupleFieldUpdateJournalRecord(record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU19CompareBinaryDecode(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := UnmarshalTupleFieldUpdateJournalRecord(tu19CompareBinaryPayload); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTU19CompareWireSize(t *testing.T) {
	t.Logf("json_bytes=%d binary_bytes=%d", len(tu19CompareJSONPayload), len(tu19CompareBinaryPayload))
}
