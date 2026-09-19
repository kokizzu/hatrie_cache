package hatReplication

import (
	"encoding/hex"
	"encoding/json"
	"testing"
)

type tu38ConflictIntrospectionJSONEvent struct {
	Sequence          uint64                        `json:"sequence"`
	Space             string                        `json:"space"`
	KeyDigest         string                        `json:"key_digest"`
	Left              ConflictVersion               `json:"left"`
	Right             ConflictVersion               `json:"right"`
	Decision          ConflictIntrospectionDecision `json:"decision"`
	TimestampUnixNano int64                         `json:"timestamp_unix_nano"`
}

var (
	tu38ConflictIntrospectionJSONBytes   []byte
	tu38ConflictIntrospectionJSONDecoded []tu38ConflictIntrospectionJSONEvent
)

func BenchmarkTU38BaselineJSONSnapshotEncode(b *testing.B) {
	events := tu38ConflictIntrospectionBenchmarkEvents(256)
	value := make([]tu38ConflictIntrospectionJSONEvent, len(events))
	for index, event := range events {
		value[index] = tu38ConflictIntrospectionJSONEvent{
			Sequence:          event.Sequence,
			Space:             event.Space,
			KeyDigest:         hex.EncodeToString(event.KeyDigest[:]),
			Left:              event.Left,
			Right:             event.Right,
			Decision:          event.Decision,
			TimestampUnixNano: event.TimestampUnixNano,
		}
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		encoded, err := json.Marshal(value)
		if err != nil {
			b.Fatal(err)
		}
		tu38ConflictIntrospectionJSONBytes = encoded
	}
	b.ReportMetric(float64(len(tu38ConflictIntrospectionJSONBytes)), "wire-bytes/op")
}

func BenchmarkTU38BaselineJSONSnapshotDecode(b *testing.B) {
	events := tu38ConflictIntrospectionBenchmarkEvents(256)
	value := make([]tu38ConflictIntrospectionJSONEvent, len(events))
	for index, event := range events {
		value[index] = tu38ConflictIntrospectionJSONEvent{
			Sequence:          event.Sequence,
			Space:             event.Space,
			KeyDigest:         hex.EncodeToString(event.KeyDigest[:]),
			Left:              event.Left,
			Right:             event.Right,
			Decision:          event.Decision,
			TimestampUnixNano: event.TimestampUnixNano,
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		var decoded []tu38ConflictIntrospectionJSONEvent
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
		tu38ConflictIntrospectionJSONDecoded = decoded
	}
	if len(tu38ConflictIntrospectionJSONDecoded) != len(value) {
		b.Fatal("JSON snapshot decode lost events")
	}
}

func tu38ConflictIntrospectionBenchmarkEvents(count int) []ConflictIntrospectionEvent {
	events := make([]ConflictIntrospectionEvent, count)
	for index := range events {
		event := tu38ConflictIntrospectionBenchmarkEvent()
		event.Sequence = uint64(index + 1)
		event.TimestampUnixNano = int64(index + 1)
		events[index] = event
	}
	return events
}
