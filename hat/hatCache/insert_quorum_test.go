package hatCache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hatrie_cache/hat/hatReplication"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestExecuteCacheCommandUsesRequestInsertQuorumWhenServerQuorumIsOff(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer target.Close()
	replicator := newCommandQuorumTestReplicator(t, target)

	trie := newTestTrie(t)
	request := CacheCommandRequest{
		Command:      "SETSTR",
		Key:          "quorum:insert",
		Value:        "value",
		InsertQuorum: 2,
	}
	response, rejected := executeCacheCommand(context.Background(), trie, request, commandExecutionOptions{
		Replicator: replicator,
	})
	if !rejected || response.OK {
		t.Fatalf("executeCacheCommand() rejected = %t, response = %#v; want insert quorum rejection", rejected, response)
	}
	if !strings.Contains(response.Message, hatReplication.ErrWriteQuorumUnsatisfied.Error()) {
		t.Fatalf("response message = %q, want write quorum error", response.Message)
	}
	if got := trie.GetString(request.Key); got != request.Value {
		t.Fatalf("local write = %q, want %q after quorum failure", got, request.Value)
	}
}

func TestExecuteCacheCommandRequestInsertQuorumCannotWeakenServerQuorum(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer target.Close()
	replicator := newCommandQuorumTestReplicator(t, target)

	request := CacheCommandRequest{
		Command:      "SETSTR",
		Key:          "quorum:minimum",
		Value:        "value",
		InsertQuorum: 1,
	}
	response, rejected := executeCacheCommand(context.Background(), newTestTrie(t), request, commandExecutionOptions{
		Replicator:  replicator,
		WriteQuorum: 2,
	})
	if !rejected || response.OK {
		t.Fatalf("executeCacheCommand() rejected = %t, response = %#v; want server quorum to remain enforced", rejected, response)
	}
}

func TestExecuteCacheCommandRejectsInvalidInsertQuorumUsage(t *testing.T) {
	tests := []struct {
		name    string
		request CacheCommandRequest
	}{
		{name: "negative", request: CacheCommandRequest{Command: "SETSTR", Key: "quorum:negative", InsertQuorum: -1}},
		{name: "batch", request: CacheCommandRequest{Command: "BATCH", InsertQuorum: 2}},
		{name: "batch item", request: CacheCommandRequest{Command: "BATCH", Batch: []CacheCommandRequest{{Command: "SETSTR", Key: "quorum:nested", Value: "value", InsertQuorum: 2}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, rejected := executeCacheCommand(context.Background(), newTestTrie(t), test.request, commandExecutionOptions{})
			if !rejected || response.OK {
				t.Fatalf("executeCacheCommand() rejected = %t, response = %#v; want invalid insert quorum", rejected, response)
			}
		})
	}
}

func TestMonitoringHandlerAcceptsJSONInsertQuorum(t *testing.T) {
	ht := newTestTrie(t)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer target.Close()
	replicator := newCommandQuorumTestReplicator(t, target)
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		Replicator: replicator,
	}).Handler()

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"quorum:http-insert","value":"value","insert_quorum":2}`)))
	if response.Code != http.StatusConflict {
		t.Fatalf("command status = %d, want %d", response.Code, http.StatusConflict)
	}
	if got := ht.GetString("quorum:http-insert"); got != "value" {
		t.Fatalf("local write = %q, want value after quorum failure", got)
	}
}

func TestGRPCCommandRequestCarriesInsertQuorum(t *testing.T) {
	request := cacheCommandRequestFromProto(&hatriecachev1.CommandRequest{
		Command:      "SETSTR",
		Key:          "quorum:grpc",
		InsertQuorum: 3,
	})
	if request.InsertQuorum != 3 {
		t.Fatalf("gRPC insert quorum = %d, want 3", request.InsertQuorum)
	}
}
