package hatReplication

import (
	"path/filepath"
	"testing"
)

var conflictEventBenchmarkSink ConflictEvent

func BenchmarkConflictResolutionBaseline(b *testing.B) {
	left := ConflictVersion{Timestamp: 10, NodeID: "west", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "east", Sequence: 2}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		winner, err := ResolveConflictVersion(left, right)
		if err != nil {
			b.Fatal(err)
		}
		conflictEventBenchmarkSink = ConflictEvent{Left: winner}
	}
}

func BenchmarkConflictResolutionWithEventLog(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{
		Capacity: 1024,
		Secret:   []byte("conflict-event-benchmark-secret"),
	})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "west", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "east", Sequence: 2}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		winner, err := ResolveConflictVersion(left, right)
		if err != nil {
			b.Fatal(err)
		}
		event, err := log.Record(ConflictEventInput{
			Space:    "orders",
			Key:      []byte("customer-42"),
			Left:     left,
			Right:    right,
			Decision: ConflictEventDecisionRightWins,
			Winner:   &winner,
		})
		if err != nil {
			b.Fatal(err)
		}
		conflictEventBenchmarkSink = event
	}
}

func BenchmarkConflictEventLogReadAfter(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{
		Capacity: 1024,
		Secret:   []byte("conflict-event-benchmark-secret"),
	})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "west", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "east", Sequence: 2}
	for index := 0; index < 1024; index++ {
		if _, err := log.Record(ConflictEventInput{
			Space:    "orders",
			Key:      []byte("customer-42"),
			Left:     left,
			Right:    right,
			Decision: ConflictEventDecisionRightWins,
			Winner:   &right,
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		page, err := log.ReadAfter(1023, 1)
		if err != nil {
			b.Fatal(err)
		}
		conflictEventBenchmarkSink = page.Events[0]
	}
}

func BenchmarkConflictEventLogDurable(b *testing.B) {
	log, err := OpenConflictEventLog(filepath.Join(b.TempDir(), "conflicts.hce"), ConflictEventLogOptions{
		Capacity: 4096,
		Secret:   []byte("conflict-event-benchmark-secret"),
	})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "west", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "east", Sequence: 2}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := log.Record(ConflictEventInput{
			Space:    "orders",
			Key:      []byte("customer-42"),
			Left:     left,
			Right:    right,
			Decision: ConflictEventDecisionRightWins,
			Winner:   &right,
		}); err != nil {
			b.Fatal(err)
		}
	}
}
