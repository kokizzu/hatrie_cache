package hatCache

import (
	"context"
	"strings"
	"testing"

	hatReplication "hatrie_cache/hat/hatReplication"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestReplicaReadOnlyGateRejectsDirectScalarBatch(t *testing.T) {
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)
	trie.UpsertString("direct", "before")

	gate := hatReplication.NewReplicaReadOnlyGate()
	trie.SetReplicaReadOnlyGate(gate)
	gate.SetReadOnly("replica maintenance")

	response := trie.executeScalarBatchDirect(context.Background(), &hatriecachev1.ScalarBatchRequest{
		BatchId:      1,
		Operations:   []hatriecachev1.ScalarCommand{hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING},
		Keys:         []string{"direct"},
		StringValues: [][]byte{[]byte("after")},
	})
	if response.GetOk() || !strings.Contains(response.GetError(), hatReplication.ErrReplicaReadOnly.Error()) {
		t.Fatalf("direct scalar response = %#v, want read-only rejection", response)
	}
	if got := trie.GetString("direct"); got != "before" {
		t.Fatalf("direct value = %q, want before", got)
	}
}

func TestReplicaReadOnlyGateRejectsDirectStructuredBatch(t *testing.T) {
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)

	gate := hatReplication.NewReplicaReadOnlyGate()
	trie.SetReplicaReadOnlyGate(gate)
	gate.SetReadOnly("replica maintenance")

	response := trie.executeStructuredBatchDirect(context.Background(), &hatriecachev1.StructuredBatchRequest{
		BatchId:    2,
		Operations: []hatriecachev1.StructuredCommand{hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP},
		Keys:       []string{"direct-map"},
		Subkeys:    []string{"field"},
		Values:     [][]byte{[]byte("value")},
	})
	if response.GetOk() || !strings.Contains(response.GetError(), hatReplication.ErrReplicaReadOnly.Error()) {
		t.Fatalf("direct structured response = %#v, want read-only rejection", response)
	}
	if trie.Exists("direct-map") {
		t.Fatal("direct structured batch created a value while read-only")
	}
}
