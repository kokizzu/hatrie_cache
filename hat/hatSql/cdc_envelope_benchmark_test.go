package hatSql

import (
	"strconv"
	"strings"
	"testing"
)

type mz015BenchmarkEnvelope struct {
	operation string
	key       string
	before    Row
	after     Row
}

type mz015BaselineChange struct {
	operation string
	key       string
	before    Row
	after     Row
}

func mz015BenchmarkEvents() []mz015BenchmarkEnvelope {
	const eventCount = 10_000
	const keyCount = 1_000

	events := make([]mz015BenchmarkEnvelope, eventCount)
	for index := range events {
		key := strconv.Itoa(index % keyCount)
		switch index % 4 {
		case 0:
			events[index] = mz015BenchmarkEnvelope{
				operation: "c",
				key:       key,
				after:     Row{"id": index % keyCount, "value": index},
			}
		case 1:
			events[index] = mz015BenchmarkEnvelope{
				operation: "u",
				key:       key,
				before:    Row{"id": index % keyCount, "value": index - 1},
				after:     Row{"id": index % keyCount, "value": index},
			}
		case 2:
			events[index] = mz015BenchmarkEnvelope{
				operation: "d",
				key:       key,
				before:    Row{"id": index % keyCount, "value": index},
			}
		default:
			events[index] = mz015BenchmarkEnvelope{
				operation: "r",
				key:       key,
				after:     Row{"id": index % keyCount, "value": index},
			}
		}
	}
	return events
}

// mz015BaselineManualNormalize represents the duplicated adapter logic that
// callers had to write before a shared CDC boundary existed.
func mz015BaselineManualNormalize(event mz015BenchmarkEnvelope) (mz015BaselineChange, bool) {
	operation := strings.TrimSpace(event.operation)
	var canonical string
	switch {
	case strings.EqualFold(operation, "i"),
		strings.EqualFold(operation, "c"),
		strings.EqualFold(operation, "create"),
		strings.EqualFold(operation, "insert"),
		strings.EqualFold(operation, "r"),
		strings.EqualFold(operation, "read"),
		strings.EqualFold(operation, "snapshot"):
		canonical = "INSERT"
	case strings.EqualFold(operation, "u"),
		strings.EqualFold(operation, "update"):
		canonical = "UPDATE"
	case strings.EqualFold(operation, "d"),
		strings.EqualFold(operation, "delete"),
		strings.EqualFold(operation, "remove"):
		canonical = "DELETE"
	case strings.EqualFold(operation, "replace"),
		strings.EqualFold(operation, "upsert"):
		switch {
		case event.before == nil && event.after != nil:
			canonical = "INSERT"
		case event.before != nil && event.after != nil:
			canonical = "UPDATE"
		default:
			return mz015BaselineChange{}, false
		}
	default:
		return mz015BaselineChange{}, false
	}

	key := strings.TrimSpace(event.key)
	if key == "" {
		return mz015BaselineChange{}, false
	}
	switch canonical {
	case "INSERT":
		if event.before != nil || event.after == nil {
			return mz015BaselineChange{}, false
		}
	case "UPDATE":
		if event.before == nil || event.after == nil {
			return mz015BaselineChange{}, false
		}
	case "DELETE":
		if event.after != nil {
			return mz015BaselineChange{}, false
		}
	}
	return mz015BaselineChange{
		operation: canonical,
		key:       key,
		before:    event.before,
		after:     event.after,
	}, true
}

var mz015BenchmarkSink int

func BenchmarkMZ015BaselineManual(b *testing.B) {
	events := mz015BenchmarkEvents()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		accepted := 0
		for _, event := range events {
			change, ok := mz015BaselineManualNormalize(event)
			if !ok {
				b.Fatal("baseline fixture unexpectedly rejected")
			}
			accepted += len(change.operation) + len(change.key)
		}
		mz015BenchmarkSink = accepted
	}
}

func BenchmarkMZ015Normalize(b *testing.B) {
	baselineEvents := mz015BenchmarkEvents()
	events := make([]CDCEnvelope, len(baselineEvents))
	for index, event := range baselineEvents {
		events[index] = CDCEnvelope{
			Operation: event.operation,
			Key:       event.key,
			Before:    event.before,
			After:     event.after,
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		accepted := 0
		for _, event := range events {
			change, err := NormalizeCDCEnvelope(event)
			if err != nil {
				b.Fatal(err)
			}
			accepted += len(change.Operation) + len(change.Key)
		}
		mz015BenchmarkSink = accepted
	}
}
