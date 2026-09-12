package hatCache

import "testing"

var (
	benchmarkJournalReadFenceValue    HatValue
	benchmarkJournalReadFenceSequence uint64
)

func BenchmarkCommandJournalPointReadThenSequence(b *testing.B) {
	journal, trie, _ := openCommandJournalSubscriptionBenchmarkFixture(b, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		value, err := trie.GetChecked("benchmark:000000")
		if err != nil {
			b.Fatal(err)
		}
		benchmarkJournalReadFenceValue = value
		benchmarkJournalReadFenceSequence = journal.Sequence()
	}
}

func BenchmarkCommandJournalReadFence(b *testing.B) {
	journal, trie, _ := openCommandJournalSubscriptionBenchmarkFixture(b, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sequence, err := journal.WithReadFence(func() error {
			value, err := trie.GetChecked("benchmark:000000")
			benchmarkJournalReadFenceValue = value
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
		benchmarkJournalReadFenceSequence = sequence
	}
}
