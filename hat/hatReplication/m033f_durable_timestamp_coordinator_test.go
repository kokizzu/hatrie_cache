package hatReplication

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDurableGlobalTimestampOraclePersistsBeforeReturning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "global-timestamps.bin")
	coordinator, err := NewDurableGlobalTimestampOracle(path, 1, 0)
	if err != nil {
		t.Fatalf("NewDurableGlobalTimestampOracle() error = %v", err)
	}
	grant, err := coordinator.Reserve(GlobalTimestampRequest{
		Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 4,
	})
	if err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if grant.Start != 1 || grant.End != 4 {
		t.Fatalf("grant = %#v, want [1,4]", grant)
	}

	reopened, err := NewDurableGlobalTimestampOracle(path, 99, 999)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	if reopened.Term() != 1 || reopened.Current() != 4 {
		t.Fatalf("reopened term/current = %d/%d, want 1/4", reopened.Term(), reopened.Current())
	}
	retry, err := reopened.Reserve(GlobalTimestampRequest{
		Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 4,
	})
	if err != nil {
		t.Fatalf("retry Reserve() error = %v", err)
	}
	if retry != grant {
		t.Fatalf("retry grant = %#v, want %#v", retry, grant)
	}
}

func TestDurableGlobalTimestampOracleDoesNotPublishWhenSaveFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "global-timestamps.bin")
	coordinator, err := NewDurableGlobalTimestampOracle(path, 1, 0)
	if err != nil {
		t.Fatalf("NewDurableGlobalTimestampOracle() error = %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove snapshot: %v", err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("replace snapshot with directory: %v", err)
	}
	_, err = coordinator.Reserve(GlobalTimestampRequest{
		Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1,
	})
	if err == nil {
		t.Fatal("Reserve() unexpectedly succeeded with a directory snapshot path")
	}
	if coordinator.Current() != 0 {
		t.Fatalf("Current() = %d after failed save, want 0", coordinator.Current())
	}
}

func BenchmarkGlobalTimestampOracleDurableReserve(b *testing.B) {
	path := filepath.Join(b.TempDir(), "global-timestamps.bin")
	coordinator, err := NewDurableGlobalTimestampOracle(path, 1, 0)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, err := coordinator.Reserve(GlobalTimestampRequest{
			Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: uint64(index + 1), Count: 64,
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
