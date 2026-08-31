package hatCache_test

import (
	"testing"

	"hatrie_cache/hat/hatCache"
)

type projectionFrontierBenchmarkJournal struct {
	sequence uint64
}

func (journal *projectionFrontierBenchmarkJournal) Tail(uint64, int) (hatCache.CommandJournalTail, error) {
	return hatCache.CommandJournalTail{}, nil
}

func (journal *projectionFrontierBenchmarkJournal) SetProjectionWatermark(_ string, sequence uint64) error {
	journal.sequence = sequence
	return nil
}

func (journal *projectionFrontierBenchmarkJournal) RemoveProjectionWatermark(string) bool {
	return true
}

func BenchmarkSQLProjectionRetentionFrontierCommit(b *testing.B) {
	frontier, err := hatCache.NewSQLProjectionRetentionFrontier("analytics", []string{"orders", "people"})
	if err != nil {
		b.Fatal(err)
	}
	journal := &projectionFrontierBenchmarkJournal{}
	b.ReportAllocs()
	b.ResetTimer()
	for sequence := uint64(1); sequence <= uint64(b.N); sequence++ {
		if err := frontier.Commit(journal, map[string]uint64{"orders": sequence, "people": sequence}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLProjectionRetentionDirectWatermark(b *testing.B) {
	journal := &projectionFrontierBenchmarkJournal{}
	b.ReportAllocs()
	b.ResetTimer()
	for sequence := uint64(1); sequence <= uint64(b.N); sequence++ {
		if err := journal.SetProjectionWatermark("analytics", sequence); err != nil {
			b.Fatal(err)
		}
	}
}
