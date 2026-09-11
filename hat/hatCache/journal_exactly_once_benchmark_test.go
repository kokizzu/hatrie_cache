package hatCache

import (
	"context"
	"testing"
	"time"
)

type commandJournalExactlyOnceBenchmarkSink struct {
	committed chan struct{}
}

func (sink *commandJournalExactlyOnceBenchmarkSink) LoadSequence(context.Context) (uint64, error) {
	return 0, nil
}

func (sink *commandJournalExactlyOnceBenchmarkSink) Begin(context.Context, uint64) (CommandJournalExactlyOnceTransaction, error) {
	return commandJournalExactlyOnceBenchmarkTransaction{sink: sink}, nil
}

type commandJournalExactlyOnceBenchmarkTransaction struct {
	sink *commandJournalExactlyOnceBenchmarkSink
}

func (transaction commandJournalExactlyOnceBenchmarkTransaction) Write(context.Context, []CommandJournalRecord) error {
	return nil
}

func (transaction commandJournalExactlyOnceBenchmarkTransaction) Commit(context.Context, uint64) error {
	select {
	case transaction.sink.committed <- struct{}{}:
	default:
	}
	return nil
}

func (commandJournalExactlyOnceBenchmarkTransaction) Rollback(context.Context) error {
	return nil
}

func BenchmarkMZ012BaselineAtLeastOnceSinkBatch100(b *testing.B) {
	journal, _, _ := openCommandJournalSubscriptionBenchmarkFixture(b, commandJournalSubscriptionBenchmarkRecords)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		sink := &commandJournalSinkBenchmarkSink{delivered: make(chan struct{}, 1)}
		runner, err := journal.StartCommandJournalSink(context.Background(), sink, CommandJournalSinkOptions{
			ReplayLimit:  commandJournalSubscriptionBenchmarkRecords,
			Buffer:       commandJournalSubscriptionBenchmarkRecords,
			PollInterval: time.Hour,
			BatchSize:    commandJournalSubscriptionBenchmarkRecords,
			BatchWait:    time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
		select {
		case <-sink.delivered:
		case <-time.After(time.Second):
			runner.Close()
			b.Fatal("timed out waiting for at-least-once sink batch")
		}
		if err := runner.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ012ExactlyOnceSinkBatch100(b *testing.B) {
	journal, _, _ := openCommandJournalSubscriptionBenchmarkFixture(b, commandJournalSubscriptionBenchmarkRecords)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		sink := &commandJournalExactlyOnceBenchmarkSink{committed: make(chan struct{}, 1)}
		runner, err := journal.StartCommandJournalExactlyOnceSink(context.Background(), sink, CommandJournalExactlyOnceSinkOptions{
			ReplayLimit:  commandJournalSubscriptionBenchmarkRecords,
			Buffer:       commandJournalSubscriptionBenchmarkRecords,
			PollInterval: time.Hour,
			BatchSize:    commandJournalSubscriptionBenchmarkRecords,
			BatchWait:    time.Hour,
		})
		if err != nil {
			b.Fatal(err)
		}
		select {
		case <-sink.committed:
		case <-time.After(time.Second):
			runner.Close()
			b.Fatal("timed out waiting for exactly-once sink batch")
		}
		if err := runner.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
