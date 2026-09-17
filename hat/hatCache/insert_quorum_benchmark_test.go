package hatCache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func BenchmarkExecuteCacheCommandInsertQuorum(b *testing.B) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	b.Cleanup(target.Close)
	replicator := newCommandQuorumTestReplicator(b, target)
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	request := CacheCommandRequest{
		Command:      "SETSTR",
		Key:          "quorum:benchmark",
		Value:        "value",
		InsertQuorum: 2,
	}
	options := commandExecutionOptions{Replicator: replicator}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, rejected := executeCacheCommand(context.Background(), trie, request, options)
		if rejected || !response.OK {
			b.Fatalf("executeCacheCommand() rejected = %t, response = %#v", rejected, response)
		}
	}
}
