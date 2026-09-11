package hatCache

import (
	"context"
	"testing"
	"time"
)

type commandJournalSinkBenchmarkSink struct {
	delivered chan struct{}
}

func (sink *commandJournalSinkBenchmarkSink) Write(context.Context, []CommandJournalRecord) error {
	select {
	case sink.delivered <- struct{}{}:
	default:
	}
	return nil
}

type commandJournalSinkBenchmarkCheckpoint struct{}

func (commandJournalSinkBenchmarkCheckpoint) Load(context.Context) (uint64, error) {
	return 0, nil
}

func (commandJournalSinkBenchmarkCheckpoint) Save(context.Context, uint64) error {
	return nil
}

func BenchmarkMZ011BaselineSubscriptionBatch100(b *testing.B) {
	journal, _, _ := openCommandJournalSubscriptionBenchmarkFixture(b, commandJournalSubscriptionBenchmarkRecords)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
			ReplayLimit:  commandJournalSubscriptionBenchmarkRecords,
			Buffer:       commandJournalSubscriptionBenchmarkRecords,
			PollInterval: time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
		for record := 0; record < commandJournalSubscriptionBenchmarkRecords; record++ {
			if _, ok := <-subscription.Records(); !ok {
				b.Fatalf("subscription closed after %d records: %v", record, subscription.Err())
			}
		}
		subscription.Close()
	}
}

func BenchmarkMZ011CommandJournalSinkBatch100(b *testing.B) {
	benchmarkMZ011CommandJournalSinkBatch100(b, nil)
}

func BenchmarkMZ011CommandJournalSinkBatch100WithCheckpoint(b *testing.B) {
	benchmarkMZ011CommandJournalSinkBatch100(b, commandJournalSinkBenchmarkCheckpoint{})
}

func benchmarkMZ011CommandJournalSinkBatch100(b *testing.B, checkpoint CommandJournalSinkCheckpointStore) {
	journal, _, _ := openCommandJournalSubscriptionBenchmarkFixture(b, commandJournalSubscriptionBenchmarkRecords)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		sink := &commandJournalSinkBenchmarkSink{delivered: make(chan struct{}, 1)}
		runner, err := journal.StartCommandJournalSink(context.Background(), sink, CommandJournalSinkOptions{
			ReplayLimit:     commandJournalSubscriptionBenchmarkRecords,
			Buffer:          commandJournalSubscriptionBenchmarkRecords,
			PollInterval:    time.Hour,
			BatchSize:       commandJournalSubscriptionBenchmarkRecords,
			BatchWait:       time.Hour,
			CheckpointStore: checkpoint,
		})
		if err != nil {
			b.Fatal(err)
		}
		select {
		case <-sink.delivered:
		case <-time.After(time.Second):
			runner.Close()
			b.Fatal("timed out waiting for sink batch")
		}
		if err := runner.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
