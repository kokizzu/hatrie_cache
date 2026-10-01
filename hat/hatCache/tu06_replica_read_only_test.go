package hatCache

import (
	"errors"
	"testing"
	"time"
)

func TestTU06ReplicaReadOnlyRejectsDirectAndCommandWrites(t *testing.T) {
	trie := newTestTrie(t)
	if trie.ReplicaReadOnly() {
		t.Fatal("new trie is unexpectedly read-only")
	}
	if err := trie.UpsertStringChecked("before", "value"); err != nil {
		t.Fatalf("initial write error = %v", err)
	}
	trie.SetReplicaReadOnly(true)
	if !trie.ReplicaReadOnly() {
		t.Fatal("read-only state did not enable")
	}
	if err := trie.UpsertStringChecked("direct", "value"); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("direct write error = %v, want ErrReplicaReadOnly", err)
	}
	if deleted, err := trie.DeleteChecked("before"); !errors.Is(err, ErrReplicaReadOnly) || deleted {
		t.Fatalf("direct delete = %v/%v, want false/ErrReplicaReadOnly", deleted, err)
	}
	if expired, err := trie.ExpireChecked("before", time.Second); !errors.Is(err, ErrReplicaReadOnly) || expired {
		t.Fatalf("direct expire = %v/%v, want false/ErrReplicaReadOnly", expired, err)
	}
	if persisted, err := trie.PersistChecked("before"); !errors.Is(err, ErrReplicaReadOnly) || persisted {
		t.Fatalf("direct persist = %v/%v, want false/ErrReplicaReadOnly", persisted, err)
	}
	if err := trie.UpsertCounterChecked("counter", 1); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("counter write error = %v, want ErrReplicaReadOnly", err)
	}
	if _, err := trie.IncrementCounterChecked("counter", 1); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("counter increment error = %v, want ErrReplicaReadOnly", err)
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "command", Value: "value"})
	if response.OK || response.Message != ErrReplicaReadOnly.Error() {
		t.Fatalf("command response = %#v, want read-only rejection", response)
	}
	response = trie.ExecuteCommand(CacheCommandRequest{Command: "DEL", Key: "before"})
	if response.OK || response.Message != ErrReplicaReadOnly.Error() {
		t.Fatalf("command delete response = %#v, want read-only rejection", response)
	}
	if _, ok, err := trie.GetStringChecked("direct"); err != nil || ok {
		t.Fatal("rejected direct write changed the trie")
	}
	if _, ok, err := trie.GetStringChecked("command"); err != nil || ok {
		t.Fatal("rejected command write changed the trie")
	}
}

func TestTU06ReplicaReadOnlyAllowsReadsAndOperatorResume(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.UpsertStringChecked("key", "before"); err != nil {
		t.Fatalf("initial write error = %v", err)
	}
	trie.SetReplicaReadOnly(true)
	value, ok, err := trie.GetStringChecked("key")
	if err != nil || !ok || value != "before" {
		t.Fatalf("read-only read = %q/%v, want before/true", value, ok)
	}
	trie.SetReplicaReadOnly(false)
	if trie.ReplicaReadOnly() {
		t.Fatal("operator resume did not disable read-only state")
	}
	if err := trie.UpsertStringChecked("key", "after"); err != nil {
		t.Fatalf("write after resume error = %v", err)
	}
}

func TestTU06ReplicaReadOnlyAllowsInternalReplication(t *testing.T) {
	trie := newTestTrie(t)
	trie.SetReplicaReadOnly(true)
	operation := snapshotOperation{entry: snapshotEntry{Key: "replicated", Type: "string", String: "value"}}
	if err := trie.commandInternalSetOperation(operation); err != nil {
		t.Fatalf("internal replication error = %v", err)
	}
	if value, ok, err := trie.GetStringChecked("replicated"); err != nil || !ok || value != "value" {
		t.Fatalf("replicated value = %q/%v, want value/true", value, ok)
	}
	if !trie.commandInternalDelete("replicated") {
		t.Fatal("internal replication delete returned false")
	}
	if _, ok, err := trie.GetStringChecked("replicated"); err != nil || ok {
		t.Fatalf("internal deleted value = %v/%v, want absent", ok, err)
	}
}

func TestTU06ReplicaReadOnlySharesStateWithLocalPartitions(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(2); err != nil {
		t.Fatalf("ConfigureLocalPartitions() error = %v", err)
	}
	trie.SetReplicaReadOnly(true)
	if err := trie.UpsertStringChecked("partitioned", "value"); !errors.Is(err, ErrReplicaReadOnly) {
		t.Fatalf("partitioned write error = %v, want ErrReplicaReadOnly", err)
	}
}

func BenchmarkTU06ReplicaReadOnly(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	b.Run("normal-write", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := trie.UpsertStringChecked("tu06-key", "value"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("rejected-write", func(b *testing.B) {
		trie.SetReplicaReadOnly(true)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := trie.UpsertStringChecked("tu06-key", "value"); !errors.Is(err, ErrReplicaReadOnly) {
				b.Fatalf("write error = %v, want ErrReplicaReadOnly", err)
			}
		}
	})
}
