package hatReplication

import (
	"testing"
	"time"
)

type legacyChangeEventBaseline struct {
	Sequence  uint64
	Namespace string
	Operation string
	Key       string
	Value     interface{}
	At        time.Time
}

type legacyChangeLogBaseline struct {
	events []legacyChangeEventBaseline
}

func (log *legacyChangeLogBaseline) append(event legacyChangeEventBaseline) {
	event.Sequence = uint64(len(log.events) + 1)
	log.events = append(log.events, event)
}

func BenchmarkLegacyChangeLogAppend(b *testing.B) {
	var log legacyChangeLogBaseline
	event := legacyChangeEventBaseline{
		Namespace: "orders",
		Operation: "upsert",
		Key:       "order:42",
		Value:     []byte("value"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		log.append(event)
	}
}
