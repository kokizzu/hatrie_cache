package hatTopology_test

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func TestTU13DurableMembershipJoinLeaveAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.log")
	log, err := hatTopology.OpenDurableMembershipLog(path, hatTopology.DurableMembershipLogOptions{})
	if err != nil {
		t.Fatalf("OpenDurableMembershipLog() error = %v", err)
	}

	first, err := log.Join(0, hatTopology.TopologyNode{ID: "node-a", Address: "127.0.0.1:9001", Role: "replica"})
	if err != nil {
		t.Fatalf("Join(node-a) error = %v", err)
	}
	if first.Generation != 1 {
		t.Fatalf("first generation = %d, want 1", first.Generation)
	}
	if _, err := log.Join(0, hatTopology.TopologyNode{ID: "node-b", Address: "127.0.0.1:9002", Role: "replica"}); !errors.Is(err, hatTopology.ErrDurableMembershipGenerationConflict) {
		t.Fatalf("stale Join() error = %v, want generation conflict", err)
	}
	if _, err := log.Join(1, hatTopology.TopologyNode{ID: "node-b", Address: "127.0.0.1:9002", Role: "replica"}); err != nil {
		t.Fatalf("Join(node-b) error = %v", err)
	}
	if _, err := log.Join(2, hatTopology.TopologyNode{ID: "node-b", Address: "127.0.0.1:9002", Role: "replica"}); !errors.Is(err, hatTopology.ErrDurableMembershipNodeExists) {
		t.Fatalf("duplicate Join() error = %v, want node exists", err)
	}
	if _, err := log.Leave(2, "node-a"); err != nil {
		t.Fatalf("Leave(node-a) error = %v", err)
	}
	snapshot, err := log.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.Generation != 3 || len(snapshot.Nodes) != 1 || snapshot.Nodes[0].ID != "node-b" {
		t.Fatalf("snapshot = %#v, want generation 3 with node-b", snapshot)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := hatTopology.OpenDurableMembershipLog(path, hatTopology.DurableMembershipLogOptions{})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()
	recovered, err := reopened.Snapshot()
	if err != nil {
		t.Fatalf("recovered Snapshot() error = %v", err)
	}
	if recovered.Generation != snapshot.Generation || len(recovered.Nodes) != 1 || recovered.Nodes[0].ID != "node-b" {
		t.Fatalf("recovered snapshot = %#v, want %#v", recovered, snapshot)
	}
}

func TestTU13DurableMembershipRejectsCorruptTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.log")
	if err := os.WriteFile(path, []byte("{\"generation\":1\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := hatTopology.OpenDurableMembershipLog(path, hatTopology.DurableMembershipLogOptions{}); !errors.Is(err, hatTopology.ErrDurableMembershipCorrupt) {
		t.Fatalf("corrupt open error = %v, want ErrDurableMembershipCorrupt", err)
	}
}

func TestTU13DurableMembershipUnsafeNoSyncIsExplicit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.log")
	log, err := hatTopology.OpenDurableMembershipLog(path, hatTopology.DurableMembershipLogOptions{UnsafeNoSync: true})
	if err != nil {
		t.Fatalf("OpenDurableMembershipLog(UnsafeNoSync) error = %v", err)
	}
	defer log.Close()
	if _, err := log.Join(0, hatTopology.TopologyNode{ID: "node-a", Address: "127.0.0.1:9001", Role: "replica"}); err != nil {
		t.Fatalf("Join() error = %v", err)
	}
}

func TestTU13DurableMembershipSnapshotIsCopiedAndCloseIsTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.log")
	log, err := hatTopology.OpenDurableMembershipLog(path, hatTopology.DurableMembershipLogOptions{})
	if err != nil {
		t.Fatalf("OpenDurableMembershipLog() error = %v", err)
	}
	if _, err := log.Join(0, hatTopology.TopologyNode{ID: "node-a", Address: "127.0.0.1:9001", Role: "replica"}); err != nil {
		t.Fatalf("Join() error = %v", err)
	}
	snapshot, err := log.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	snapshot.Nodes[0].ID = "caller-mutated"
	unchanged, err := log.Snapshot()
	if err != nil {
		t.Fatalf("second Snapshot() error = %v", err)
	}
	if unchanged.Nodes[0].ID != "node-a" {
		t.Fatalf("snapshot mutation changed log state: %#v", unchanged.Nodes)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := log.Snapshot(); !errors.Is(err, hatTopology.ErrDurableMembershipClosed) {
		t.Fatalf("Snapshot() after close error = %v, want closed", err)
	}
}

func BenchmarkTU13DurableMembershipJoin(b *testing.B) {
	b.Run("unsafe-no-sync", func(b *testing.B) {
		benchmarkTU13DurableMembershipJoin(b, true)
	})
	b.Run("durable-fsync", func(b *testing.B) {
		benchmarkTU13DurableMembershipJoin(b, false)
	})
}

func benchmarkTU13DurableMembershipJoin(b *testing.B, unsafeNoSync bool) {
	path := filepath.Join(b.TempDir(), "membership.log")
	log, err := hatTopology.OpenDurableMembershipLog(path, hatTopology.DurableMembershipLogOptions{UnsafeNoSync: unsafeNoSync})
	if err != nil {
		b.Fatalf("OpenDurableMembershipLog() error = %v", err)
	}
	defer log.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		id := "node-" + strconv.Itoa(index)
		if _, err := log.Join(uint64(index), hatTopology.TopologyNode{ID: id, Address: "127.0.0.1:9001", Role: "replica"}); err != nil {
			b.Fatalf("Join() error = %v", err)
		}
	}
}
