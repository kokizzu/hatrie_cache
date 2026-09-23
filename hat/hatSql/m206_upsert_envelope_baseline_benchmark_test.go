package hatSql

import (
	"strconv"
	"testing"
)

type m206BaselineUpsertEnvelope struct {
	Sequence uint64
	Key      string
	Row      Row
}

func BenchmarkM206UpsertEnvelopeBaseline(b *testing.B) {
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
			row := change.After
			if change.Operation == CDCOperationDelete {
				row = nil
			}
			envelope := m206BaselineUpsertEnvelope{Sequence: change.Sequence, Key: change.Key, Row: row}
			accepted += len(envelope.Key) + len(envelope.Row)
		}
		if accepted == 0 {
			b.Fatal("benchmark fixture unexpectedly empty")
		}
	}
}

func BenchmarkM206DebeziumUpsertEnvelopeBaseline(b *testing.B) {
	events := make([]m206BaselineUpsertEnvelope, 256)
	for index := range events {
		events[index] = m206BaselineUpsertEnvelope{
			Sequence: uint64(index),
			Key:      "region-a|id=" + strconv.Itoa(index),
			Row:      Row{"id": index, "value": index},
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		accepted := 0
		for _, envelope := range events {
			accepted += len(envelope.Key) + len(envelope.Row)
		}
		if accepted == 0 {
			b.Fatal("benchmark fixture unexpectedly empty")
		}
	}
}
