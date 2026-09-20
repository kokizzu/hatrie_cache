package hatCache

import (
	"context"
	"testing"
)

var (
	tu39BenchmarkRecord = CommandJournalRecord{
		Sequence: 42,
		Request: CacheCommandRequest{
			Command: "SETSTR",
			Key:     "orders/1",
			Value:   "paid",
		},
	}
	tu39BenchmarkRawRecord   CommandJournalRecord
	tu39BenchmarkChangeEvent SpaceChangefeedEvent
)

func BenchmarkTU39RawJournalRecordNext(b *testing.B) {
	records := make(chan CommandJournalRecord, 1)
	records <- tu39BenchmarkRecord
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		record := <-records
		records <- record
		tu39BenchmarkRawRecord = record
	}
}

func BenchmarkTU39SpaceChangefeedNext(b *testing.B) {
	records := make(chan CommandJournalRecord, 1)
	records <- tu39BenchmarkRecord
	feed := &SpaceChangefeed{
		subscription:  &CommandJournalSubscription{records: records},
		name:          "orders",
		schemaVersion: 1,
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		event, ok, err := feed.Next(ctx)
		if err != nil || !ok {
			b.Fatalf("Next() = %#v, %v, %v", event, ok, err)
		}
		records <- tu39BenchmarkRecord
		tu39BenchmarkChangeEvent = event
	}
}
