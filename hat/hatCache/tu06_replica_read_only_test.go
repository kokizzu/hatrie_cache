package hatCache

import (
	"errors"
	"strings"
	"testing"

	hatReplication "hatrie_cache/hat/hatReplication"
)

func TestReplicaReadOnlyGateRejectsPublicWrites(t *testing.T) {
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)
	trie.UpsertString("existing", "before")

	gate := hatReplication.NewReplicaReadOnlyGate()
	trie.SetReplicaReadOnlyGate(gate)
	gate.SetReadOnly("replica maintenance")

	if err := trie.UpsertStringChecked("existing", "after"); !errors.Is(err, hatReplication.ErrReplicaReadOnly) {
		t.Fatalf("UpsertStringChecked() error = %v, want ErrReplicaReadOnly", err)
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "existing", Value: "after"})
	if response.OK || !strings.Contains(response.Message, hatReplication.ErrReplicaReadOnly.Error()) {
		t.Fatalf("ExecuteCommand() response = %#v, want read-only rejection", response)
	}
	if got := trie.GetString("existing"); got != "before" {
		t.Fatalf("existing value = %q, want before", got)
	}

	gate.SetWritable()
	if err := trie.UpsertStringChecked("existing", "after"); err != nil {
		t.Fatalf("UpsertStringChecked() after SetWritable error = %v", err)
	}
}

func BenchmarkTU06UpsertStringBaseline(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := trie.UpsertStringChecked("benchmark-key", "value"); err != nil {
			b.Fatal(err)
		}
	}
}

func TestReplicaReadOnlyGateAllowsReadsAndInternalReplication(t *testing.T) {
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)
	trie.UpsertString("read", "value")
	trie.UpsertString("internal", "value")

	gate := hatReplication.NewReplicaReadOnlyGate()
	trie.SetReplicaReadOnlyGate(gate)
	gate.SetReadOnly("replica maintenance")

	if got := trie.GetString("read"); got != "value" {
		t.Fatalf("GetString() = %q, want value", got)
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "read"})
	if !response.OK || response.Value != "value" {
		t.Fatalf("read command response = %#v, want value", response)
	}
	if removed := trie.deleteInternal("internal"); !removed {
		t.Fatal("deleteInternal() = false, want internal replication delete to bypass gate")
	}
	if got := trie.GetString("internal"); got != "" {
		t.Fatalf("internal value = %q after deleteInternal(), want empty", got)
	}
}

func BenchmarkTU06UpsertStringWritableGate(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	trie.SetReplicaReadOnlyGate(hatReplication.NewReplicaReadOnlyGate())
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := trie.UpsertStringChecked("benchmark-key", "value"); err != nil {
			b.Fatal(err)
		}
	}
}
