package hatCache

import (
	"context"
	"testing"
	"time"
)

func BenchmarkCommandJournalSubscriptionSkipReplay(b *testing.B) {
	journal, _, _ := openCommandJournalSubscriptionBenchmarkFixture(b, commandJournalSubscriptionBenchmarkRecords)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
			SkipReplay:   true,
			Buffer:       1,
			PollInterval: time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
		subscription.Close()
	}
}
