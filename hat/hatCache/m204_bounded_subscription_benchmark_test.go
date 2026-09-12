package hatCache

import "testing"

func BenchmarkCommandJournalSubscriptionNotifyUnbounded(b *testing.B) {
	benchmarkCommandJournalSubscriptionNotify(b, 0)
}

func BenchmarkCommandJournalSubscriptionNotifyBounded(b *testing.B) {
	benchmarkCommandJournalSubscriptionNotify(b, 1<<20)
}

func benchmarkCommandJournalSubscriptionNotify(b *testing.B, upToSequence uint64) {
	journal := &CommandJournal{}
	subscription := &CommandJournalSubscription{
		events:       make(chan CommandJournalRecord, 1),
		upToSequence: upToSequence,
	}
	journal.registerCommandJournalSubscription(subscription)
	b.Cleanup(func() { journal.unregisterCommandJournalSubscription(subscription) })
	record := CommandJournalRecord{
		Sequence: 1,
		Request: CacheCommandRequest{
			Key: "benchmark:key",
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		select {
		case <-subscription.events:
		default:
		}
		journal.notifyCommandJournalSubscriptions(record)
	}
}
