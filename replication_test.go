package hatriecache

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type trackingReadCloser struct {
	reader  *strings.Reader
	closed  bool
	drained bool
}

func newTrackingReadCloser(value string) *trackingReadCloser {
	return &trackingReadCloser{reader: strings.NewReader(value)}
}

func (body *trackingReadCloser) Read(data []byte) (int, error) {
	n, err := body.reader.Read(data)
	if err != nil {
		body.drained = true
	}
	return n, err
}

func (body *trackingReadCloser) Close() error {
	body.closed = true
	return nil
}

func TestHTTPReplicatorReplicatesSetAndDeleteToOwners(t *testing.T) {
	var requests []CacheCommandRequest
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests = append(requests, request)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"})
	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("set replication result = %#v, want one ok target", result)
	}

	response = trie.ExecuteCommand(CacheCommandRequest{Command: "DEL", Key: "session:1"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "DEL", Key: "session:1"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("delete replication result = %#v, want one ok target", result)
	}

	if len(requests) != 2 {
		t.Fatalf("replicated requests len = %d, want 2", len(requests))
	}
	if requests[0].Command != "INTERNALSET" || requests[0].Key != "session:1" || requests[0].Value == "" {
		t.Fatalf("first replicated request = %#v, want INTERNALSET with snapshot", requests[0])
	}
	if requests[1].Command != "INTERNALDEL" || requests[1].Key != "session:1" {
		t.Fatalf("second replicated request = %#v, want INTERNALDEL", requests[1])
	}
	if got := replicator.LastResult(); !reflect.DeepEqual(got, result) {
		t.Fatalf("LastResult() = %#v, want last result %#v", got, result)
	}
}

func TestHTTPReplicatorSkipsWhenNotLeaderOrInternalCommand(t *testing.T) {
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, "127.0.0.1:1")
	election := NewElectionStore(topology, ElectionOptions{})
	if err := election.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(node-a) error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
	})

	trie.UpsertString("session:1", "value")
	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, CacheCommandResponse{OK: true})
	if !result.Skipped || result.Reason != "local node is not elected leader" {
		t.Fatalf("not leader result = %#v, want skipped not leader", result)
	}

	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "INTERNALDEL", Key: "session:1"}, CacheCommandResponse{OK: true})
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("internal command result = %#v, want skipped internal command", result)
	}
}

func TestReplicationPayloadKindUsesJournaledMutationClassification(t *testing.T) {
	ttl := int64(30)
	priority := int64(5)
	for _, tt := range []struct {
		name    string
		request CacheCommandRequest
		want    replicationPayloadKind
	}{
		{
			name:    "read command",
			request: CacheCommandRequest{Command: "GET", Key: "key"},
			want:    replicationPayloadNone,
		},
		{
			name:    "failed write response",
			request: CacheCommandRequest{Command: "SETSTR", Key: "key", Value: "value"},
			want:    replicationPayloadNone,
		},
		{
			name:    "internal set",
			request: CacheCommandRequest{Command: "INTERNALSET", Key: "key", Value: `{"type":"string","string":"value"}`},
			want:    replicationPayloadNone,
		},
		{
			name:    "internal delete",
			request: CacheCommandRequest{Command: "INTERNALDEL", Key: "key"},
			want:    replicationPayloadNone,
		},
		{
			name:    "delete",
			request: CacheCommandRequest{Command: "DEL", Key: "key"},
			want:    replicationPayloadDelete,
		},
		{
			name:    "ttl alias",
			request: CacheCommandRequest{Command: "SETSTRX", Key: "key", Value: "value", TTLSeconds: &ttl},
			want:    replicationPayloadSet,
		},
		{
			name:    "priority queue alias",
			request: CacheCommandRequest{Command: "PUSHPRIORITY", Key: "jobs", Value: "job", Priority: &priority},
			want:    replicationPayloadSet,
		},
		{
			name:    "top-k alias",
			request: CacheCommandRequest{Command: "TOPKRESERVE", Key: "top", Value: "3"},
			want:    replicationPayloadSet,
		},
		{
			name:    "quantile alias",
			request: CacheCommandRequest{Command: "QADD", Key: "latency", Value: "12.5"},
			want:    replicationPayloadSet,
		},
		{
			name:    "fenwick alias",
			request: CacheCommandRequest{Command: "FWADD", Key: "scores", Values: Slice{json.Number("2"), json.Number("7")}},
			want:    replicationPayloadSet,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response := CacheCommandResponse{OK: true}
			if tt.name == "failed write response" {
				response.OK = false
			}
			if got := replicationPayloadKindFor(tt.request, response); got != tt.want {
				t.Fatalf("replicationPayloadKindFor(%#v) = %v, want %v", tt.request, got, tt.want)
			}
		})
	}
}

func TestHTTPReplicatorSkipsCanceledContextBeforeNetwork(t *testing.T) {
	called := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called <- struct{}{}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value")
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := replicator.ReplicateCommand(ctx, trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, CacheCommandResponse{OK: true})
	if !result.Skipped || result.Reason != context.Canceled.Error() {
		t.Fatalf("canceled replication result = %#v, want skipped context canceled", result)
	}
	select {
	case <-called:
		t.Fatal("canceled replication reached remote target")
	default:
	}
	if got := replicator.LastResult(); !reflect.DeepEqual(got, result) {
		t.Fatalf("LastResult() = %#v, want canceled result %#v", got, result)
	}
}

func TestHTTPReplicatorAcceptsNilContext(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"})
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	result := replicator.ReplicateCommand(nil, trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("nil context replication result = %#v, want one ok target", result)
	}
	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "session:1" || request.Value == "" {
			t.Fatalf("nil context replicated request = %#v, want INTERNALSET snapshot", request)
		}
	default:
		t.Fatal("nil context replication did not reach remote target")
	}
}

func TestHTTPReplicatorAsyncQueuesMaterializedPayload(t *testing.T) {
	release := make(chan struct{})
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 2,
	})
	defer replicator.Close()

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "first"}
	response := trie.ExecuteCommand(write)
	result := replicator.ReplicateCommand(context.Background(), trie, write, response)
	if !result.Queued || result.Skipped || len(result.Targets) != 1 {
		t.Fatalf("async enqueue result = %#v, want one queued target", result)
	}
	trie.UpsertString("session:1", "second")
	close(release)

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "session:1" {
			t.Fatalf("async request = %#v, want INTERNALSET session:1", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("async snapshot JSON error = %v", err)
		}
		if entry.String != "first" {
			t.Fatalf("async snapshot string = %q, want first", entry.String)
		}
	case <-time.After(time.Second):
		t.Fatal("async replication did not deliver queued request")
	}
	final := waitForReplicationLastResult(t, replicator, func(result ReplicationResult) bool {
		return !result.Queued && !result.Skipped && len(result.Targets) == 1 && result.Targets[0].OK
	})
	if final.Queue == nil || !final.Queue.Enabled || final.Queue.Capacity != 2 || final.Queue.Enqueued != 1 || final.Queue.Attempts != 1 || final.Queue.Successes != 1 || final.Queue.Failures != 0 || final.Queue.Dropped != 0 {
		t.Fatalf("async queue stats = %#v, want one successful queued delivery", final.Queue)
	}
}

func TestHTTPReplicatorAsyncRetriesFailedDelivery(t *testing.T) {
	attempts := make(chan struct{}, 2)
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			attempts <- struct{}{}
			if len(attempts) == 1 {
				return nil, errors.New("temporary failure")
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"ok":true,"message":"ok"}`)),
				Request:    request,
			}, nil
		}),
	}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, "http://node-b.local")
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             client,
		AsyncQueueSize:     2,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Millisecond,
	})
	defer replicator.Close()

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}
	response := trie.ExecuteCommand(write)
	result := replicator.ReplicateCommand(context.Background(), trie, write, response)
	if !result.Queued || result.Skipped {
		t.Fatalf("async enqueue result = %#v, want queued", result)
	}
	final := waitForReplicationLastResult(t, replicator, func(result ReplicationResult) bool {
		return !result.Queued && len(result.Targets) == 1 && result.Targets[0].OK
	})
	if got := len(attempts); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if final.Queue == nil || final.Queue.Enqueued != 1 || final.Queue.Attempts != 2 || final.Queue.Successes != 1 || final.Queue.Failures != 1 || final.Queue.Retried != 1 {
		t.Fatalf("retry queue stats = %#v, want one failed attempt, one retry, one success", final.Queue)
	}
}

func TestHTTPReplicatorAsyncReportsFullQueue(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-release
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()
	defer close(release)

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
	})
	defer replicator.Close()

	first := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "one"}
	firstResponse := trie.ExecuteCommand(first)
	if result := replicator.ReplicateCommand(context.Background(), trie, first, firstResponse); !result.Queued || result.Skipped {
		t.Fatalf("first enqueue result = %#v, want queued", result)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start first queued delivery")
	}
	second := CacheCommandRequest{Command: "SETSTR", Key: "session:2", Value: "two"}
	secondResponse := trie.ExecuteCommand(second)
	if result := replicator.ReplicateCommand(context.Background(), trie, second, secondResponse); !result.Queued || result.Skipped {
		t.Fatalf("second enqueue result = %#v, want queued while worker is blocked", result)
	}
	third := CacheCommandRequest{Command: "SETSTR", Key: "session:3", Value: "three"}
	thirdResponse := trie.ExecuteCommand(third)
	result := replicator.ReplicateCommand(context.Background(), trie, third, thirdResponse)
	if !result.Skipped || result.Reason != "replication queue is full" || result.Queued {
		t.Fatalf("third enqueue result = %#v, want full queue skip", result)
	}
	last := replicator.LastResult()
	if last.Queue == nil || last.Queue.Enqueued != 2 || last.Queue.Dropped != 1 || last.Queue.Depth != 1 || last.Queue.Capacity != 1 {
		t.Fatalf("full queue stats = %#v, want two enqueued, one dropped, one pending", last.Queue)
	}
}

func TestHTTPReplicatorAsyncCloseIsIdempotentAndRejectsEnqueue(t *testing.T) {
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, "http://node-b.local")
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		AsyncQueueSize: 1,
	})
	replicator.Close()
	replicator.Close()

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}
	response := trie.ExecuteCommand(write)
	result := replicator.ReplicateCommand(context.Background(), trie, write, response)
	if !result.Skipped || result.Reason != "replication queue is closed" || result.Queued {
		t.Fatalf("post-close replicate result = %#v, want closed queue skip", result)
	}
	last := replicator.LastResult()
	if last.Queue == nil || !last.Queue.Closed || last.Queue.Dropped != 1 {
		t.Fatalf("closed queue stats = %#v, want closed with one dropped enqueue", last.Queue)
	}
}

func TestHTTPReplicatorUsesTopologyWhenElectionUnconfigured(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"})
	topology := replicationTestTopology(t, target.URL)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Client:   target.Client(),
	})

	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("topology-only replication result = %#v, want one ok target", result)
	}
	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "session:1" || request.Value == "" {
			t.Fatalf("topology-only replicated request = %#v, want INTERNALSET snapshot", request)
		}
	default:
		t.Fatal("topology-only replication did not reach remote target")
	}

	follower := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-b",
		Topology: topology,
		Client:   target.Client(),
	})
	result = follower.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, response)
	if !result.Skipped || result.Reason != "local node is not elected leader" {
		t.Fatalf("topology-only follower result = %#v, want skipped not leader", result)
	}
}

func TestHTTPReplicatorSyncAllReplicatesLeaderOwnedEntries(t *testing.T) {
	requests := make(chan CacheCommandRequest, 2)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value-1")
	trie.UpsertString("session:2", "value-2")
	trie.UpsertString("other:1", "ignored")
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	result := replicator.SyncAll(context.Background(), trie, "session:")
	if result.Skipped || result.Command != "SYNC" || result.Key != "session:" || result.Entries != 2 || len(result.Targets) != 2 {
		t.Fatalf("sync result = %#v, want two synced entries", result)
	}
	targetKeys := map[string]bool{}
	for _, target := range result.Targets {
		if !target.OK || target.Key == "" {
			t.Fatalf("sync target = %#v, want ok target with key", target)
		}
		targetKeys[target.Key] = true
	}
	if !targetKeys["session:1"] || !targetKeys["session:2"] {
		t.Fatalf("sync target keys = %#v, want session keys", targetKeys)
	}
	for i := 0; i < 2; i++ {
		request := <-requests
		if request.Command != "INTERNALSET" || request.Value == "" || (request.Key != "session:1" && request.Key != "session:2") {
			t.Fatalf("sync request = %#v, want INTERNALSET session snapshot", request)
		}
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected sync request = %#v", request)
	default:
	}
	if got := replicator.LastResult(); !reflect.DeepEqual(got, result) {
		t.Fatalf("LastResult() = %#v, want sync result %#v", got, result)
	}
}

func TestHTTPReplicatorSyncAllSkipsNoEntries(t *testing.T) {
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, "127.0.0.1:1")
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
	})

	result := replicator.SyncAll(context.Background(), trie, "missing:")
	if !result.Skipped || result.Reason != "no entries to sync" || result.Entries != 0 || len(result.Targets) != 0 {
		t.Fatalf("empty sync result = %#v, want no entries skip", result)
	}
	if got := replicator.LastResult(); !reflect.DeepEqual(got, result) {
		t.Fatalf("LastResult() = %#v, want empty sync result %#v", got, result)
	}
}

func TestPostReplicationCommandDrainsErrorResponseBody(t *testing.T) {
	body := newTrackingReadCloser(strings.Repeat("error body ", 32))
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Status:     "503 Service Unavailable",
				Header:     make(http.Header),
				Body:       body,
				Request:    request,
			}, nil
		}),
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: client})

	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: "127.0.0.1:8080",
	}, CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: "{}"})
	if result.OK || result.Status != http.StatusServiceUnavailable || result.Error != "503 Service Unavailable" {
		t.Fatalf("postReplicationCommand() = %#v, want 503 error result", result)
	}
	if !body.drained || !body.closed {
		t.Fatalf("response body drained=%v closed=%v, want both true", body.drained, body.closed)
	}
}

func waitForReplicationLastResult(t *testing.T, replicator *HTTPReplicator, accept func(ReplicationResult) bool) ReplicationResult {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var result ReplicationResult
	for time.Now().Before(deadline) {
		result = replicator.LastResult()
		if accept(result) {
			return result
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("LastResult() = %#v did not satisfy predicate", result)
	return ReplicationResult{}
}

func TestHTTPReplicatorReplicatesBloomFilterMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATEBF", Key: "seen", Value: "1000", Subkey: "0.001"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATEBF response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDBF", Key: "seen", Values: Slice{"alpha", "beta"}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDBF response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Bloom filter replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "seen" || request.Value == "" {
			t.Fatalf("replicated Bloom request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated Bloom snapshot JSON error = %v", err)
		}
		if entry.Type != "bloom_filter" || entry.BloomFilter == nil {
			t.Fatalf("replicated Bloom snapshot = %#v, want bloom_filter payload", entry)
		}
	default:
		t.Fatal("Bloom filter mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASBF", Key: "seen", Value: "alpha"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASBF", Key: "seen", Value: "alpha"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Bloom filter read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesXorFilterMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "CREATEXF", Key: "seen", Value: "8"}); !response.OK {
		t.Fatalf("CREATEXF response = %#v, want ok", response)
	}
	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "ADDXF", Key: "seen", Values: Slice{"alpha", "beta"}}); !response.OK {
		t.Fatalf("ADDXF response = %#v, want ok", response)
	}
	build := CacheCommandRequest{Command: "BUILDXF", Key: "seen"}
	response := trie.ExecuteCommand(build)
	if !response.OK {
		t.Fatalf("BUILDXF response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, build, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("XOR filter replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "seen" || request.Value == "" {
			t.Fatalf("replicated XOR request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated XOR snapshot JSON error = %v", err)
		}
		if entry.Type != "xor_filter" || entry.XorFilter == nil || !entry.XorFilter.Built {
			t.Fatalf("replicated XOR snapshot = %#v, want built xor_filter payload", entry)
		}
	default:
		t.Fatal("XOR filter mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASXF", Key: "seen", Value: "alpha"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASXF", Key: "seen", Value: "alpha"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("XOR filter read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesCuckooFilterMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATECF", Key: "seen", Value: "128", Subkey: "0.001"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATECF response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDCF", Key: "seen", Values: Slice{"alpha", "beta"}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDCF response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Cuckoo filter replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "seen" || request.Value == "" {
			t.Fatalf("replicated Cuckoo request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated Cuckoo snapshot JSON error = %v", err)
		}
		if entry.Type != "cuckoo_filter" || entry.CuckooFilter == nil {
			t.Fatalf("replicated Cuckoo snapshot = %#v, want cuckoo_filter payload", entry)
		}
	default:
		t.Fatal("Cuckoo filter mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASCF", Key: "seen", Value: "alpha"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASCF", Key: "seen", Value: "alpha"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Cuckoo filter read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesRoaringBitmapMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATERB", Key: "ids"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATERB response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDRB", Key: "ids", Values: Slice{json.Number("1"), json.Number("65543")}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDRB response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Roaring bitmap replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "ids" || request.Value == "" {
			t.Fatalf("replicated Roaring bitmap request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated Roaring bitmap snapshot JSON error = %v", err)
		}
		if entry.Type != "roaring_bitmap" || entry.RoaringBitmap == nil {
			t.Fatalf("replicated Roaring bitmap snapshot = %#v, want roaring_bitmap payload", entry)
		}
	default:
		t.Fatal("Roaring bitmap mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASRB", Key: "ids", Value: "1"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASRB", Key: "ids", Value: "1"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Roaring bitmap read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesCountMinSketchMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATECMS", Key: "freq", Value: "128", Subkey: "4"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATECMS response = %#v, want ok", response)
	}
	increment := CacheCommandRequest{Command: "INCRCMS", Key: "freq", Value: "alpha", Subkey: "3"}
	response := trie.ExecuteCommand(increment)
	if !response.OK {
		t.Fatalf("INCRCMS response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, increment, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Count-Min Sketch replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "freq" || request.Value == "" {
			t.Fatalf("replicated Count-Min Sketch request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated Count-Min Sketch snapshot JSON error = %v", err)
		}
		if entry.Type != "count_min_sketch" || entry.CountMinSketch == nil {
			t.Fatalf("replicated Count-Min Sketch snapshot = %#v, want count_min_sketch payload", entry)
		}
	default:
		t.Fatal("Count-Min Sketch mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "ESTCMS", Key: "freq", Value: "alpha"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "ESTCMS", Key: "freq", Value: "alpha"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Count-Min Sketch read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesHyperLogLogMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATEHLL", Key: "card", Value: "10"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATEHLL response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDHLL", Key: "card", Values: Slice{"alpha", "beta"}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDHLL response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("HyperLogLog replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "card" || request.Value == "" {
			t.Fatalf("replicated HyperLogLog request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated HyperLogLog snapshot JSON error = %v", err)
		}
		if entry.Type != "hyperloglog" || entry.HyperLogLog == nil {
			t.Fatalf("replicated HyperLogLog snapshot = %#v, want hyperloglog payload", entry)
		}
	default:
		t.Fatal("HyperLogLog mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "COUNTHLL", Key: "card"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "COUNTHLL", Key: "card"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("HyperLogLog read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesTopKMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATETOPK", Key: "top", Value: "3"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATETOPK response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDTOPK", Key: "top", Value: "alpha", Subkey: "5"}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDTOPK response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Top-K replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "top" || request.Value == "" {
			t.Fatalf("replicated Top-K request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated Top-K snapshot JSON error = %v", err)
		}
		if entry.Type != "top_k" || entry.TopK == nil {
			t.Fatalf("replicated Top-K snapshot = %#v, want top_k payload", entry)
		}
	default:
		t.Fatal("Top-K mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "GETTOPK", Key: "top"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "GETTOPK", Key: "top"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Top-K read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesReservoirSampleMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATERS", Key: "sample", Value: "3"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATERS response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDRS", Key: "sample", Values: Slice{"alpha", "beta", "gamma", "delta"}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDRS response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("reservoir sample replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "sample" || request.Value == "" {
			t.Fatalf("replicated reservoir sample request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated reservoir sample snapshot JSON error = %v", err)
		}
		if entry.Type != "reservoir_sample" || entry.ReservoirSample == nil {
			t.Fatalf("replicated reservoir sample snapshot = %#v, want reservoir_sample payload", entry)
		}
	default:
		t.Fatal("reservoir sample mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "GETRS", Key: "sample"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "GETRS", Key: "sample"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("reservoir sample read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesQuantileSketchMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATEQ", Key: "latency", Value: "0.01"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATEQ response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDQ", Key: "latency", Values: Slice{json.Number("10"), json.Number("20"), json.Number("30")}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDQ response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("quantile sketch replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "latency" || request.Value == "" {
			t.Fatalf("replicated quantile sketch request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated quantile sketch snapshot JSON error = %v", err)
		}
		if entry.Type != "quantile_sketch" || entry.QuantileSketch == nil {
			t.Fatalf("replicated quantile sketch snapshot = %#v, want quantile_sketch payload", entry)
		}
	default:
		t.Fatal("quantile sketch mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "ESTQ", Key: "latency", Value: "0.5"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "ESTQ", Key: "latency", Value: "0.5"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("quantile sketch read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesFenwickTreeMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	create := CacheCommandRequest{Command: "CREATEFW", Key: "scores", Value: "8"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATEFW response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDFW", Key: "scores", Value: "2", Subkey: "5"}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDFW response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("Fenwick tree replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "scores" || request.Value == "" {
			t.Fatalf("replicated Fenwick tree request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated Fenwick tree snapshot JSON error = %v", err)
		}
		if entry.Type != "fenwick_tree" || entry.FenwickTree == nil {
			t.Fatalf("replicated Fenwick tree snapshot = %#v, want fenwick_tree payload", entry)
		}
	default:
		t.Fatal("Fenwick tree mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "SUMFW", Key: "scores", Value: "2"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SUMFW", Key: "scores", Value: "2"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("Fenwick tree read replication result = %#v, want skipped read command", result)
	}
}

func TestReplicationEndpointNormalizesAddresses(t *testing.T) {
	got, err := replicationEndpoint("127.0.0.1:8080/base/")
	if err != nil {
		t.Fatalf("replicationEndpoint() error = %v", err)
	}
	if got != "http://127.0.0.1:8080/base/api/commands" {
		t.Fatalf("endpoint = %q, want normalized API path", got)
	}
}

func replicationTestTopology(t *testing.T, replicaAddress string) *TopologyStore {
	t.Helper()
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://127.0.0.1:1"},
			{ID: "node-b", Address: replicaAddress},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	return topology
}
