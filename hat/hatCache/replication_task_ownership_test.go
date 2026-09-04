package hatCache

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestHTTPReplicatorCloseWithContextPreservesTaskOwnership(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()
	defer releaseOnce.Do(func() { close(release) })

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
		Timeout:        time.Minute,
	})

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:ownership", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("async replication target was not called")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := replicator.CloseWithContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseWithContext() error = %v, want context deadline", err)
	}
	if replicator.asyncClosed() {
		t.Fatal("CloseWithContext() canceled owned work after its drain deadline")
	}

	releaseOnce.Do(func() { close(release) })
	if err := replicator.CloseWithContext(context.Background()); err != nil {
		t.Fatalf("CloseWithContext(final) error = %v", err)
	}
}
