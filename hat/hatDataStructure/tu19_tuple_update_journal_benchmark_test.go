package hatDataStructure_test

import (
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

type tu19BenchmarkJSONRecord struct {
	Sequence uint64                              `json:"sequence"`
	Key      string                              `json:"key"`
	Updates  []hatDataStructure.TupleFieldUpdate `json:"updates"`
}

var tu19BenchmarkRecord = hatDataStructure.TupleFieldUpdateJournalRecord{
	Sequence: 42,
	Key:      "orders/42",
	Updates: []hatDataStructure.TupleFieldUpdate{
		{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("order-42")},
		{Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 3},
		{Index: 2, Kind: hatDataStructure.TupleFieldSplice, Start: 1, Remove: 2, Insert: []byte("XX")},
	},
}

var tu19BenchmarkJSONPayload []byte
var tu19BenchmarkBinaryPayload []byte
var tu19BenchmarkRecordSink hatDataStructure.TupleFieldUpdateJournalRecord

func init() {
	var err error
	tu19BenchmarkJSONPayload, err = json.Marshal(tu19BenchmarkJSONRecord{
		Sequence: tu19BenchmarkRecord.Sequence,
		Key:      tu19BenchmarkRecord.Key,
		Updates:  tu19BenchmarkRecord.Updates,
	})
	if err != nil {
		panic(err)
	}
	tu19BenchmarkBinaryPayload, err = hatDataStructure.MarshalTupleFieldUpdateJournalRecord(tu19BenchmarkRecord)
	if err != nil {
		panic(err)
	}
}

func BenchmarkTupleFieldUpdateJournalJSONEncode(b *testing.B) {
	value := tu19BenchmarkJSONRecord{Sequence: tu19BenchmarkRecord.Sequence, Key: tu19BenchmarkRecord.Key, Updates: tu19BenchmarkRecord.Updates}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		payload, err := json.Marshal(value)
		if err != nil {
			b.Fatal(err)
		}
		tu19BenchmarkJSONPayload = payload
	}
}

func BenchmarkTupleFieldUpdateJournalJSONDecode(b *testing.B) {
	var value tu19BenchmarkJSONRecord
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if err := json.Unmarshal(tu19BenchmarkJSONPayload, &value); err != nil {
			b.Fatal(err)
		}
	}
	tu19BenchmarkRecordSink = hatDataStructure.TupleFieldUpdateJournalRecord{Sequence: value.Sequence, Key: value.Key, Updates: value.Updates}
}

func BenchmarkTupleFieldUpdateJournalBinaryEncode(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		payload, err := hatDataStructure.MarshalTupleFieldUpdateJournalRecord(tu19BenchmarkRecord)
		if err != nil {
			b.Fatal(err)
		}
		tu19BenchmarkBinaryPayload = payload
	}
}

func BenchmarkTupleFieldUpdateJournalBinaryDecode(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		record, err := hatDataStructure.UnmarshalTupleFieldUpdateJournalRecord(tu19BenchmarkBinaryPayload)
		if err != nil {
			b.Fatal(err)
		}
		tu19BenchmarkRecordSink = record
	}
}

func TestTupleFieldUpdateJournalWireSize(t *testing.T) {
	t.Logf("json_bytes=%d binary_bytes=%d", len(tu19BenchmarkJSONPayload), len(tu19BenchmarkBinaryPayload))
}
