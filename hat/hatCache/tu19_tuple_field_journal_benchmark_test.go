package hatCache

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

var benchmarkTU19PayloadSink int

func benchmarkTU19Payload(b *testing.B) []byte {
	b.Helper()
	format, err := hatDataStructure.NewTupleFormat(17, []hatDataStructure.TupleFieldSpec{
		{Name: "count", Type: hatDataStructure.TupleFieldInt64},
		{Name: "name", Type: hatDataStructure.TupleFieldString},
		{Name: "payload", Type: hatDataStructure.TupleFieldBytes},
	})
	if err != nil {
		b.Fatalf("NewTupleFormat() error = %v", err)
	}
	tuple, err := format.PackVersioned([]hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleInt64(10),
		hatDataStructure.TupleString("before"),
		hatDataStructure.TupleBytes([]byte("abcd")),
	})
	if err != nil {
		b.Fatalf("PackVersioned() error = %v", err)
	}
	payload, err := hatDataStructure.MarshalVersionedTuple(tuple)
	if err != nil {
		b.Fatalf("MarshalVersionedTuple() error = %v", err)
	}
	return payload
}

func BenchmarkTU19TupleCommandPayloadSizes(b *testing.B) {
	payload := benchmarkTU19Payload(b)
	encoded := base64.StdEncoding.EncodeToString(payload)
	setRequest := CacheCommandRequest{
		Command: "TUPLESET",
		Key:     "order:1",
		Value:   encoded,
	}
	updateRequest := CacheCommandRequest{
		Command: "TUPLEUPDATE",
		Key:     "order:1",
		Values: []any{
			map[string]any{"index": 0, "kind": "add_int64", "delta": 5},
			map[string]any{"index": 1, "kind": "set", "value": base64.StdEncoding.EncodeToString([]byte("after"))},
			map[string]any{"index": 2, "kind": "splice", "start": 1, "remove": 2, "insert": base64.StdEncoding.EncodeToString([]byte("XY"))},
		},
	}
	setJSON, err := json.Marshal(setRequest)
	if err != nil {
		b.Fatalf("json.Marshal(TUPLESET) error = %v", err)
	}
	updateJSON, err := json.Marshal(updateRequest)
	if err != nil {
		b.Fatalf("json.Marshal(TUPLEUPDATE) error = %v", err)
	}
	b.ResetTimer()
	b.ReportMetric(float64(len(payload)), "tuple-bytes")
	b.ReportMetric(float64(len(setJSON)), "tupleset-json-bytes")
	b.ReportMetric(float64(len(updateJSON)), "tupleupdate-json-bytes")
	b.ReportMetric(float64(len(updateJSON))/float64(len(setJSON)), "update-over-set")
	for i := 0; i < b.N; i++ {
		benchmarkTU19PayloadSink = len(updateJSON)
	}
}
