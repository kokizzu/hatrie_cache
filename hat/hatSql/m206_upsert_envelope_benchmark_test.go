package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkM206AsUpsertEnvelope(b *testing.B) {
	events := make([]CDCChange, 256)
	for index := range events {
		operation := CDCOperationUpdate
		before := Row{"id": index, "value": index - 1}
		after := Row{"id": index, "value": index}
		if index%4 == 0 {
			operation = CDCOperationInsert
			before = nil
		} else if index%4 == 3 {
			operation = CDCOperationDelete
			after = nil
		}
		events[index] = CDCChange{Sequence: uint64(index), Operation: operation, Key: "key", Before: before, After: after}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		accepted := 0
		for _, change := range events {
			envelope, err := change.AsUpsertEnvelope()
			if err != nil {
				b.Fatal(err)
			}
			accepted += len(envelope.Key) + len(envelope.Row)
		}
		if accepted == 0 {
			b.Fatal("benchmark fixture unexpectedly empty")
		}
	}
}

func BenchmarkM206DebeziumAsUpsertEnvelope(b *testing.B) {
	events := m206DebeziumEvents(true)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		accepted := 0
		for _, change := range events {
			envelope, err := change.AsUpsertEnvelope()
			if err != nil {
				b.Fatal(err)
			}
			accepted += len(envelope.Key) + len(envelope.Row)
		}
		if accepted == 0 {
			b.Fatal("benchmark fixture unexpectedly empty")
		}
	}
}

func BenchmarkM206DebeziumAsUpsertEnvelopeFallback(b *testing.B) {
	events := m206DebeziumEvents(false)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		accepted := 0
		for _, change := range events {
			envelope, err := change.AsUpsertEnvelope()
			if err != nil {
				b.Fatal(err)
			}
			accepted += len(envelope.Key) + len(envelope.Row)
		}
		if accepted == 0 {
			b.Fatal("benchmark fixture unexpectedly empty")
		}
	}
}

func m206DebeziumEvents(includeStableKey bool) []DebeziumChange {
	events := make([]DebeziumChange, 256)
	for index := range events {
		stableKey := ""
		if includeStableKey {
			stableKey = "region-a|id=" + strconv.Itoa(index)
		}
		events[index] = DebeziumChange{
			Key:       Row{"tenant": "region-a", "id": index},
			StableKey: stableKey,
			Payload: DebeziumPayload{
				Op:     DebeziumUpdate,
				Before: Row{"id": index, "value": index - 1},
				After:  Row{"id": index, "value": index},
			},
		}
	}
	return events
}
