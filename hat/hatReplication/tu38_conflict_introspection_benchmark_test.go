package hatReplication

import (
	"crypto/sha256"
	"testing"
)

var (
	tu38ConflictIntrospectionEventSink ConflictIntrospectionEvent
	tu38ConflictIntrospectionBytes     []byte
	tu38ConflictIntrospectionEvents    []ConflictIntrospectionEvent
)

func BenchmarkTU38ConflictIntrospectionRecord(b *testing.B) {
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionLogOptions{MaxEvents: 1024})
	if err != nil {
		b.Fatal(err)
	}
	event := tu38ConflictIntrospectionBenchmarkEvent()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		recorded, err := log.Record(event)
		if err != nil {
			b.Fatal(err)
		}
		tu38ConflictIntrospectionEventSink = recorded
	}
}

func BenchmarkTU38ConflictIntrospectionReplay(b *testing.B) {
	log := tu38ConflictIntrospectionBenchmarkLog(b, 1024)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		events, err := log.Replay(512, 256)
		if err != nil {
			b.Fatal(err)
		}
		tu38ConflictIntrospectionEvents = events
	}
}

func BenchmarkTU38ConflictIntrospectionMarshal(b *testing.B) {
	log := tu38ConflictIntrospectionBenchmarkLog(b, 256)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		encoded, err := log.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		tu38ConflictIntrospectionBytes = encoded
	}
	b.ReportMetric(float64(len(tu38ConflictIntrospectionBytes)), "wire-bytes/op")
}

func BenchmarkTU38ConflictIntrospectionUnmarshal(b *testing.B) {
	log := tu38ConflictIntrospectionBenchmarkLog(b, 256)
	encoded, err := log.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	restored, err := NewConflictIntrospectionLog(ConflictIntrospectionLogOptions{MaxEvents: 256})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if err := restored.UnmarshalBinary(encoded); err != nil {
			b.Fatal(err)
		}
	}
	if restored.LatestSequence() == 0 {
		b.Fatal("unmarshal lost sequence")
	}
}

func tu38ConflictIntrospectionBenchmarkLog(b *testing.B, count int) *ConflictIntrospectionLog {
	b.Helper()
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionLogOptions{MaxEvents: count})
	if err != nil {
		b.Fatal(err)
	}
	event := tu38ConflictIntrospectionBenchmarkEvent()
	for index := 0; index < count; index++ {
		event.KeyDigest = sha256.Sum256([]byte("key-" + string(rune(index))))
		if _, err := log.Record(event); err != nil {
			b.Fatal(err)
		}
	}
	return log
}

func tu38ConflictIntrospectionBenchmarkEvent() ConflictIntrospectionEvent {
	return ConflictIntrospectionEvent{
		Space:     "orders",
		KeyDigest: sha256.Sum256([]byte("key-0")),
		Left:      ConflictVersion{Timestamp: 10, NodeID: "region-a", Sequence: 1},
		Right:     ConflictVersion{Timestamp: 11, NodeID: "region-b", Sequence: 2},
		Decision:  ConflictIntrospectionRightWins,
	}
}
