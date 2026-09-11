package hatCache

import (
	"context"
	"path/filepath"
	"testing"
)

func BenchmarkMZ013BaselinePersistenceBarrier(b *testing.B) {
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := journal.WithPersistenceBarrier(func(uint64) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

type mz013BenchmarkSourceCheckpointStore struct{}

func (mz013BenchmarkSourceCheckpointStore) Load(context.Context, string) (CommandJournalSourceCheckpoint, error) {
	return CommandJournalSourceCheckpoint{}, nil
}

func (mz013BenchmarkSourceCheckpointStore) Save(context.Context, string, CommandJournalSourceCheckpoint) error {
	return nil
}

func BenchmarkMZ013SourceCheckpointCommit(b *testing.B) {
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	coordinator, err := NewCommandJournalSourceCheckpointCoordinator(journal, mz013BenchmarkSourceCheckpointStore{})
	if err != nil {
		b.Fatal(err)
	}
	offset := []byte{0, 1, 255}

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := coordinator.Commit(context.Background(), "orders-eu", offset); err != nil {
			b.Fatal(err)
		}
	}
}
