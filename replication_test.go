package hatriecache

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/internal/jsonwire"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func mustDecodeReplicationTestCommand(t *testing.T, w http.ResponseWriter, r *http.Request) CacheCommandRequest {
	t.Helper()
	request, _, closeBody, ok := monitoringCommandRequest(w, r)
	if !ok {
		t.Fatal("decode replication command request failed")
	}
	defer closeBody()
	return request
}

func mustDecodeReplicationBatchValues(t *testing.T, request CacheCommandRequest) []CacheCommandRequest {
	t.Helper()
	if request.Command != "INTERNALBATCH" {
		t.Fatalf("replication request command = %q, want INTERNALBATCH", request.Command)
	}
	if len(request.Batch) > 0 {
		return append([]CacheCommandRequest(nil), request.Batch...)
	}
	out := make([]CacheCommandRequest, 0, len(request.Values))
	for idx, value := range request.Values {
		text, ok := value.(string)
		if !ok {
			t.Fatalf("batch value %d = %T, want string", idx, value)
		}
		var payload CacheCommandRequest
		if err := json.Unmarshal([]byte(text), &payload); err != nil {
			t.Fatalf("batch value %d JSON error = %v", idx, err)
		}
		out = append(out, payload)
	}
	return out
}

func replicationRequestKeys(t *testing.T, request CacheCommandRequest) []string {
	t.Helper()
	switch request.Command {
	case "INTERNALSET", "INTERNALDEL":
		return []string{request.Key}
	case "INTERNALBATCH":
		payloads := mustDecodeReplicationBatchValues(t, request)
		keys := make([]string, 0, len(payloads))
		for _, payload := range payloads {
			if payload.Command != "INTERNALSET" && payload.Command != "INTERNALDEL" {
				t.Fatalf("batch payload = %#v, want internal replication command", payload)
			}
			keys = append(keys, payload.Key)
		}
		return keys
	default:
		t.Fatalf("replication request = %#v, want internal replication command", request)
		return nil
	}
}

func assertReplicationResultTiming(t *testing.T, result ReplicationResult) {
	t.Helper()
	if result.StartedAt == nil || result.FinishedAt == nil {
		t.Fatalf("replication timing = %v/%v, want started and finished timestamps", result.StartedAt, result.FinishedAt)
	}
	if result.StartedAt.IsZero() || result.FinishedAt.IsZero() || result.FinishedAt.Before(*result.StartedAt) {
		t.Fatalf("replication timing = %s/%s, want ordered non-zero timestamps", result.StartedAt, result.FinishedAt)
	}
	if result.DurationMillis < 0 {
		t.Fatalf("replication duration = %d, want non-negative", result.DurationMillis)
	}
}

func TestReplicationHealthScoresQueueState(t *testing.T) {
	healthy := withReplicationHealth(ReplicationResult{
		Queue: &ReplicationQueueStats{Enabled: true, Depth: 0, Capacity: 8},
	})
	if healthy.Health != "ok" || healthy.HealthScore != 100 || healthy.HealthReason != "healthy" {
		t.Fatalf("healthy replication = %#v, want ok 100", healthy)
	}

	degraded := withReplicationHealth(ReplicationResult{
		Queue: &ReplicationQueueStats{Enabled: true, Depth: 6, Capacity: 8},
	})
	if degraded.Health != "degraded" || degraded.HealthScore >= healthy.HealthScore {
		t.Fatalf("degraded replication = %#v, want lower degraded score", degraded)
	}

	unhealthy := withReplicationHealth(ReplicationResult{
		Queue: &ReplicationQueueStats{
			Enabled:               true,
			Depth:                 8,
			Capacity:              8,
			Dropped:               2,
			Attempts:              10,
			Failures:              8,
			OldestQueuedAgeMillis: int64((6 * time.Minute).Milliseconds()),
		},
	})
	if unhealthy.Health != "unhealthy" || unhealthy.HealthScore >= 60 {
		t.Fatalf("unhealthy replication = %#v, want unhealthy score below 60", unhealthy)
	}

	disabled := withReplicationHealth(ReplicationResult{Skipped: true, Reason: "replication is not configured"})
	if disabled.Health != "disabled" || disabled.HealthScore != 0 {
		t.Fatalf("disabled replication = %#v, want disabled 0", disabled)
	}
}

func writeReplicationTestCommandResponse(w http.ResponseWriter, r *http.Request, response CacheCommandResponse) {
	writeCommandResponseWire(w, r, http.StatusOK, response, CommandWireFormatJSON)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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

func TestHTTPReplicatorSendsReplicationAuthToken(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer replica-secret" {
			t.Fatalf("Authorization = %q, want bearer replication token", got)
		}
		if got := r.Header.Get("X-Hatrie-Replication-Token"); got != "replica-secret" {
			t.Fatalf("X-Hatrie-Replication-Token = %q, want replication token", got)
		}
		_ = mustDecodeReplicationTestCommand(t, w, r)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:      "node-a",
		Topology:  topology,
		Election:  election,
		Client:    target.Client(),
		AuthToken: " replica-secret ",
	})

	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"})
	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("replication result = %#v, want one authenticated ok target", result)
	}
}

func TestHTTPReplicatorAnnotatesReplicationSafetyMetadata(t *testing.T) {
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080"},
			{ID: "node-b", Address: "http://node-b:8080"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a", Topology: topology})

	first := replicator.annotateReplicationPayload(CacheCommandRequest{Command: "INTERNALSET", Key: "k", Value: "{}"})
	second := replicator.annotateReplicationPayload(CacheCommandRequest{Command: "INTERNALDEL", Key: "k"})
	if first.Pairs[replicationMetaSourceNode] != "node-a" || second.Pairs[replicationMetaSourceNode] != "node-a" {
		t.Fatalf("source metadata = %#v/%#v, want node-a", first.Pairs, second.Pairs)
	}
	if first.Pairs[replicationMetaTopologyFingerprint] != topology.Fingerprint() || second.Pairs[replicationMetaTopologyFingerprint] != topology.Fingerprint() {
		t.Fatalf("topology fingerprint metadata = %#v/%#v, want %q", first.Pairs, second.Pairs, topology.Fingerprint())
	}
	firstSequence, err := commandUint64Value(first.Pairs[replicationMetaSequence])
	if err != nil {
		t.Fatalf("first sequence metadata error = %v", err)
	}
	secondSequence, err := commandUint64Value(second.Pairs[replicationMetaSequence])
	if err != nil {
		t.Fatalf("second sequence metadata error = %v", err)
	}
	if firstSequence == 0 || secondSequence != firstSequence+1 {
		t.Fatalf("replication sequences = %d/%d, want monotonically increasing", firstSequence, secondSequence)
	}
}

func TestExecuteCacheCommandSkipsDuplicateReplicationSequence(t *testing.T) {
	topology, err := NewTopologyStore(SingleNodeTopology("node-b", ""))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	safety := NewReplicationSafetyStore()
	ht := CreateHatTrie()
	defer ht.Destroy()
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("session:1", "replicated")
	dump := source.ExecuteCommand(CacheCommandRequest{Command: "DUMP", Key: "session:1"})
	if !dump.OK || dump.Value == "" {
		t.Fatalf("source dump = %#v, want snapshot", dump)
	}

	request := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:1",
		Value:   dump.Value,
		Pairs: Map{
			replicationMetaSourceNode:          "node-a",
			replicationMetaSequence:            "1",
			replicationMetaTopologyFingerprint: topology.Fingerprint(),
		},
	}
	response, rejected := executeCacheCommand(context.Background(), ht, request, commandExecutionOptions{
		Topology:          topology,
		ReplicationSafety: safety,
	})
	if rejected || !response.OK {
		t.Fatalf("first replication response = %#v rejected=%v, want ok", response, rejected)
	}
	duplicate := request
	duplicate.Command = "INTERNALDEL"
	duplicate.Value = ""
	response, rejected = executeCacheCommand(context.Background(), ht, duplicate, commandExecutionOptions{
		Topology:          topology,
		ReplicationSafety: safety,
	})
	if rejected || !response.OK || response.Message != "duplicate replication command" {
		t.Fatalf("duplicate replication response = %#v rejected=%v, want duplicate ok", response, rejected)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "session:1"}); !got.OK || got.Value != "replicated" {
		t.Fatalf("GET after duplicate replication = %#v, want original value", got)
	}
}

func TestExecuteCacheCommandRejectsReplicationTopologyMismatch(t *testing.T) {
	localTopology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080"},
			{ID: "node-b", Address: "http://node-b:9090"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore(local) error = %v", err)
	}
	remoteTopology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080"},
			{ID: "node-b", Address: "http://node-b:8080"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore(remote) error = %v", err)
	}
	ht := CreateHatTrie()
	defer ht.Destroy()

	response, rejected := executeCacheCommand(context.Background(), ht, CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:1",
		Value:   `{"type":"string","string":"replicated"}`,
		Pairs: Map{
			replicationMetaSourceNode:          "node-a",
			replicationMetaSequence:            "1",
			replicationMetaTopologyFingerprint: remoteTopology.Fingerprint(),
		},
	}, commandExecutionOptions{
		Topology:          localTopology,
		ReplicationSafety: NewReplicationSafetyStore(),
	})
	if !rejected || response.OK || !strings.Contains(response.Message, "topology fingerprint mismatch") {
		t.Fatalf("topology mismatch response = %#v rejected=%v, want rejected mismatch", response, rejected)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "session:1"}); got.Value != "" {
		t.Fatalf("GET after rejected replication = %#v, want missing value", got)
	}
}

func TestExecuteCacheCommandAllowsReplicationFingerprintWithoutClusterTopology(t *testing.T) {
	localTopology, err := NewTopologyStore(SingleNodeTopology("node-b", ""))
	if err != nil {
		t.Fatalf("NewTopologyStore(local) error = %v", err)
	}
	remoteTopology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeSharded,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a:8080"},
			{ID: "node-b", Address: "http://node-b:8080"},
		},
		Shards: []TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore(remote) error = %v", err)
	}
	ht := CreateHatTrie()
	defer ht.Destroy()

	response, rejected := executeCacheCommand(context.Background(), ht, CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:1",
		Value:   `{"type":"string","string":"replicated"}`,
		Pairs: Map{
			replicationMetaSourceNode:          "node-a",
			replicationMetaSequence:            "1",
			replicationMetaTopologyFingerprint: remoteTopology.Fingerprint(),
		},
	}, commandExecutionOptions{
		Topology:          localTopology,
		ReplicationSafety: NewReplicationSafetyStore(),
	})
	if rejected || !response.OK {
		t.Fatalf("single-node replication response = %#v rejected=%v, want ok", response, rejected)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "session:1"}); !got.OK || got.Value != "replicated" {
		t.Fatalf("GET after single-node replication = %#v, want replicated value", got)
	}
}

func TestHTTPReplicatorUsesProtobufWireByDefault(t *testing.T) {
	var gotRequest CacheCommandRequest
	var gotContentType string
	var gotAccept string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		gotAccept = r.Header.Get("Accept")
		gotRequest = mustDecodeReplicationTestCommand(t, w, r)
		writeReplicationTestCommandResponse(w, r, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Client: target.Client(),
	})
	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: target.URL,
	}, CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: `{"type":"string","string":"value"}`})

	if !result.OK || result.Error != "" {
		t.Fatalf("postReplicationCommand() = %#v, want protobuf ok", result)
	}
	if gotContentType != commandWireContentTypeProtobuf || gotAccept != commandWireContentTypeProtobuf {
		t.Fatalf("wire headers content-type/accept = %q/%q, want protobuf", gotContentType, gotAccept)
	}
	if gotRequest.Command != "INTERNALSET" || gotRequest.Key != "session:1" || gotRequest.Value == "" {
		t.Fatalf("protobuf replicated request = %#v, want INTERNALSET snapshot", gotRequest)
	}
}

func TestHTTPReplicatorFallsBackToJSONForStructuredPayload(t *testing.T) {
	var gotContentType string
	var gotAccept string
	var gotRequest CacheCommandRequest
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		gotAccept = r.Header.Get("Accept")
		gotRequest = mustDecodeReplicationTestCommand(t, w, r)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Client: target.Client(),
	})
	request := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:structured",
		Pairs: Map{
			"profile": Map{
				"name": "ivi",
				"tags": Slice{"alpha", "beta"},
			},
		},
	}
	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: target.URL,
	}, request)

	if !result.OK || result.Error != "" {
		t.Fatalf("postReplicationCommand() = %#v, want JSON fallback ok", result)
	}
	if gotContentType != commandWireContentTypeJSON || gotAccept != commandWireContentTypeJSON {
		t.Fatalf("wire headers content-type/accept = %q/%q, want JSON fallback", gotContentType, gotAccept)
	}
	if !reflect.DeepEqual(gotRequest, request) {
		t.Fatalf("replicated structured request = %#v, want %#v", gotRequest, request)
	}
}

func TestHTTPReplicatorUsesConfiguredJSONWire(t *testing.T) {
	var gotContentType string
	var gotAccept string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		gotAccept = r.Header.Get("Accept")
		request := mustDecodeReplicationTestCommand(t, w, r)
		if request.Command != "INTERNALDEL" || request.Key != "session:1" {
			t.Fatalf("json fallback request = %#v, want INTERNALDEL", request)
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Client:     target.Client(),
		WireFormat: CommandWireFormatJSON,
	})
	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: target.URL,
	}, CacheCommandRequest{Command: "INTERNALDEL", Key: "session:1"})

	if !result.OK || result.Error != "" {
		t.Fatalf("postReplicationCommand() = %#v, want configured JSON ok", result)
	}
	if gotContentType != commandWireContentTypeJSON || gotAccept != commandWireContentTypeJSON {
		t.Fatalf("wire headers content-type/accept = %q/%q, want json", gotContentType, gotAccept)
	}
}

func TestHTTPReplicatorNilReceiverReportsNotConfigured(t *testing.T) {
	var replicator *HTTPReplicator
	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value")

	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "session:1",
		Value:   "value",
	}, CacheCommandResponse{OK: true})
	if !result.Skipped || result.Command != "SETSTR" || result.Key != "session:1" || result.Reason != "replication is not configured" {
		t.Fatalf("ReplicateCommand(nil) = %#v, want not configured skip", result)
	}

	result = replicator.SyncAll(context.Background(), trie, "session:")
	if !result.Skipped || result.Command != "SYNC" || result.Key != "session:" || result.Reason != "replication is not configured" {
		t.Fatalf("SyncAll(nil) = %#v, want not configured sync skip", result)
	}
	if got := replicator.LastResult(); !got.Skipped || got.Reason != "replication is not configured" {
		t.Fatalf("LastResult(nil) = %#v, want not configured skip", got)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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
	if final.Queue.LastRetryAt == nil || final.Queue.LastRetryAgeMillis < 0 || final.Queue.FailuresByTarget["node-b"] != 1 {
		t.Fatalf("retry visibility stats = %#v, want retry timestamp and node-b failure count", final.Queue)
	}
}

func TestHTTPReplicatorAsyncRecordsDeadLetterAfterExhaustedRetries(t *testing.T) {
	attempts := make(chan struct{}, 4)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case attempts <- struct{}{}:
		default:
		}
		http.Error(w, "still down", http.StatusBadGateway)
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             target.Client(),
		AsyncQueueSize:     1,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Millisecond,
	})
	defer replicator.Close()

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:dead", Value: "secret"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}

	final := waitForReplicationLastResult(t, replicator, func(result ReplicationResult) bool {
		return result.DeadLetterCount == 1
	})
	if final.Queue == nil || final.Queue.Failures != 2 || final.Queue.Retried != 1 {
		t.Fatalf("queue stats = %#v, want two failures and one retry", final.Queue)
	}
	if len(final.DeadLetters) != 1 {
		t.Fatalf("dead letters = %#v, want one", final.DeadLetters)
	}
	deadLetter := final.DeadLetters[0]
	if deadLetter.ID != 1 || deadLetter.Command != "SETSTR" || deadLetter.Key != "session:dead" || deadLetter.Attempts != 2 || deadLetter.FailedAt == nil {
		t.Fatalf("dead letter = %#v, want SETSTR session:dead with attempts", deadLetter)
	}
	if len(deadLetter.Targets) != 1 || deadLetter.Targets[0].Node != "node-b" || deadLetter.Targets[0].OK {
		t.Fatalf("dead letter targets = %#v, want failed node-b target", deadLetter.Targets)
	}
	if strings.Contains(deadLetter.Reason, "secret") {
		t.Fatalf("dead letter reason leaked command value: %#v", deadLetter)
	}
	if got := len(attempts); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if final.Health == "ok" || final.HealthScore >= 100 {
		t.Fatalf("health = %s/%d/%q, want dead-letter failure to reduce health", final.Health, final.HealthScore, final.HealthReason)
	}
}

func TestHTTPReplicatorAsyncDeadLettersAreBounded(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "still down", http.StatusBadGateway)
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                 "node-a",
		Topology:             topology,
		Election:             election,
		Client:               target.Client(),
		AsyncQueueSize:       2,
		AsyncMaxAttempts:     1,
		AsyncDeadLetterLimit: 1,
	})
	defer replicator.Close()

	for _, key := range []string{"session:first", "session:second"} {
		write := CacheCommandRequest{Command: "SETSTR", Key: key, Value: "value"}
		response := trie.ExecuteCommand(write)
		if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
			t.Fatalf("enqueue %s result = %#v, want queued", key, result)
		}
	}

	final := waitForReplicationLastResult(t, replicator, func(result ReplicationResult) bool {
		return result.DeadLetterCount == 1 && len(result.DeadLetters) == 1 && result.DeadLetters[0].Key == "session:second"
	})
	if final.DeadLetters[0].ID != 2 {
		t.Fatalf("dead letters = %#v, want only newest id 2 retained", final.DeadLetters)
	}
}

func TestHTTPReplicatorAsyncOutboxReplaysPendingJobAfterRestart(t *testing.T) {
	var mode atomic.Int32
	entered := make(chan struct{}, 1)
	delivered := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode.Load() == 0 {
			select {
			case entered <- struct{}{}:
			default:
			}
			http.Error(w, "temporarily down", http.StatusBadGateway)
			return
		}
		request := mustDecodeReplicationTestCommand(t, w, r)
		delivered <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.json")
	outbox, err := OpenReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenReplicationOutbox() error = %v", err)
	}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             target.Client(),
		AsyncQueueSize:     1,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Hour,
		AsyncOutbox:        outbox,
	})

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:outbox", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first replication attempt did not start")
	}
	replicator.Close()

	reopened, err := OpenReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenReplicationOutbox(reopen) error = %v", err)
	}
	if jobs := reopened.jobs(); len(jobs) != 1 || jobs[0].result.Key != "session:outbox" {
		t.Fatalf("persisted jobs = %#v, want queued session:outbox job", jobs)
	}
	mode.Store(1)
	replayed := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
		AsyncOutbox:    reopened,
	})
	defer replayed.Close()

	select {
	case request := <-delivered:
		if request.Command != "INTERNALSET" || request.Key != "session:outbox" || request.Value == "" {
			t.Fatalf("replayed request = %#v, want materialized INTERNALSET", request)
		}
	case <-time.After(time.Second):
		t.Fatal("persisted replication job was not replayed")
	}
	waitUntil(t, time.Second, func() bool {
		return len(reopened.jobs()) == 0
	})
}

func TestHTTPReplicatorAsyncOutboxPersistsDeadLetters(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "still down", http.StatusBadGateway)
	}))
	defer target.Close()

	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.json")
	outbox, err := OpenReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenReplicationOutbox() error = %v", err)
	}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:             "node-a",
		Topology:         topology,
		Election:         election,
		Client:           target.Client(),
		AsyncQueueSize:   1,
		AsyncMaxAttempts: 1,
		AsyncOutbox:      outbox,
	})

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:dead-outbox", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	final := waitForReplicationLastResult(t, replicator, func(result ReplicationResult) bool {
		return result.DeadLetterCount == 1
	})
	if len(final.DeadLetters) != 1 || final.DeadLetters[0].Key != "session:dead-outbox" {
		t.Fatalf("dead letters = %#v, want persisted key", final.DeadLetters)
	}
	replicator.Close()

	reopened, err := OpenReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenReplicationOutbox(reopen) error = %v", err)
	}
	replayed := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
		AsyncOutbox:    reopened,
	})
	defer replayed.Close()
	last := replayed.LastResult()
	if last.DeadLetterCount != 1 || len(last.DeadLetters) != 1 || last.DeadLetters[0].Key != "session:dead-outbox" {
		t.Fatalf("restored dead letters = %#v count=%d, want retained session:dead-outbox", last.DeadLetters, last.DeadLetterCount)
	}
	if jobs := reopened.jobs(); len(jobs) != 0 {
		t.Fatalf("persisted jobs after dead-letter = %#v, want none", jobs)
	}
}

func TestLevelDBReplicationOutboxPersistsJobsAndDeadLetters(t *testing.T) {
	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox() error = %v", err)
	}
	job := replicationJob{
		id: 1,
		result: ReplicationResult{
			Command: "SETSTR",
			Key:     "session:leveldb",
		},
		tasks: []replicationTask{{
			target:  TopologyNode{ID: "node-b", Address: "http://node-b"},
			payload: CacheCommandRequest{Command: "INTERNALSET", Key: "session:leveldb", Value: `{"type":"string","string":"value"}`},
		}},
		enqueuedAt: time.Now().UTC(),
	}
	if err := outbox.putJob(job); err != nil {
		t.Fatalf("putJob() error = %v", err)
	}
	if err := outbox.setDeadLetters(7, []ReplicationDeadLetter{{ID: 7, Key: "session:dead", Attempts: 2}}); err != nil {
		t.Fatalf("setDeadLetters() error = %v", err)
	}
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox(reopen) error = %v", err)
	}
	defer reopened.Close()
	jobs := reopened.jobs()
	if len(jobs) != 1 || jobs[0].id != 1 || jobs[0].result.Key != "session:leveldb" || len(jobs[0].tasks) != 1 || jobs[0].tasks[0].payload.Command != "INTERNALSET" {
		t.Fatalf("jobs() = %#v, want persisted leveldb job", jobs)
	}
	deadSeq, deadLetters := reopened.deadLetters()
	if deadSeq != 7 || len(deadLetters) != 1 || deadLetters[0].Key != "session:dead" {
		t.Fatalf("deadLetters() = %d/%#v, want persisted dead letter", deadSeq, deadLetters)
	}
	if err := reopened.deleteJob(1); err != nil {
		t.Fatalf("deleteJob() error = %v", err)
	}
	if jobs := reopened.jobs(); len(jobs) != 0 {
		t.Fatalf("jobs() after delete = %#v, want none", jobs)
	}
}

func TestLevelDBReplicationOutboxSkipsCorruptPartialJobRecords(t *testing.T) {
	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox() error = %v", err)
	}
	valid := replicationJob{
		id: 2,
		result: ReplicationResult{
			Command: "SETSTR",
			Key:     "session:valid",
		},
		tasks: []replicationTask{{
			target:  TopologyNode{ID: "node-b", Address: "http://node-b"},
			payload: CacheCommandRequest{Command: "INTERNALSET", Key: "session:valid", Value: `{"type":"string","string":"value"}`},
		}},
		enqueuedAt: time.Now().UTC(),
	}
	if err := outbox.putJob(valid); err != nil {
		t.Fatalf("putJob(valid) error = %v", err)
	}
	if err := outbox.db.Put(replicationOutboxLevelDBJobKey(1), []byte(`{"id":1,"result":`), nil); err != nil {
		t.Fatalf("put corrupt job error = %v", err)
	}
	if jobs := outbox.jobs(); len(jobs) != 1 || jobs[0].id != 2 || jobs[0].result.Key != "session:valid" {
		t.Fatalf("jobs() with corrupt record = %#v, want only valid job", jobs)
	}
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox(reopen) error = %v", err)
	}
	defer reopened.Close()
	if jobs := reopened.jobs(); len(jobs) != 1 || jobs[0].id != 2 || jobs[0].result.Key != "session:valid" {
		t.Fatalf("reopened jobs() with corrupt record = %#v, want only valid job", jobs)
	}
}

func TestHTTPReplicatorLevelDBOutboxReplaysPendingJobAfterRestart(t *testing.T) {
	var mode atomic.Int32
	entered := make(chan struct{}, 1)
	delivered := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode.Load() == 0 {
			select {
			case entered <- struct{}{}:
			default:
			}
			http.Error(w, "temporarily down", http.StatusBadGateway)
			return
		}
		request := mustDecodeReplicationTestCommand(t, w, r)
		delivered <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox() error = %v", err)
	}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             target.Client(),
		AsyncQueueSize:     1,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Hour,
		AsyncOutbox:        outbox,
	})

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:leveldb-outbox", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first replication attempt did not start")
	}
	replicator.Close()
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close(outbox) error = %v", err)
	}

	reopened, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox(reopen) error = %v", err)
	}
	if jobs := reopened.jobs(); len(jobs) != 1 || jobs[0].result.Key != "session:leveldb-outbox" {
		t.Fatalf("persisted jobs = %#v, want queued session:leveldb-outbox job", jobs)
	}
	mode.Store(1)
	replayed := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 1,
		AsyncOutbox:    reopened,
	})
	defer reopened.Close()
	defer replayed.Close()

	select {
	case request := <-delivered:
		if request.Command != "INTERNALSET" || request.Key != "session:leveldb-outbox" || request.Value == "" {
			t.Fatalf("replayed request = %#v, want materialized INTERNALSET", request)
		}
	case <-time.After(time.Second):
		t.Fatal("persisted replication job was not replayed")
	}
	waitUntil(t, time.Second, func() bool {
		return len(reopened.jobs()) == 0
	})
}

func TestHTTPReplicatorLevelDBOutboxReplaysPendingJobsInOrderAfterRetryInterrupted(t *testing.T) {
	var mode atomic.Int32
	attempted := make(chan string, 2)
	delivered := make(chan string, 2)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		if mode.Load() == 0 {
			attempted <- request.Key
			http.Error(w, "temporarily down", http.StatusBadGateway)
			return
		}
		delivered <- request.Key
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	outboxPath := filepath.Join(t.TempDir(), "replication-outbox.leveldb")
	outbox, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox() error = %v", err)
	}
	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             target.Client(),
		AsyncQueueSize:     2,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Hour,
		AsyncOutbox:        outbox,
	})

	for _, key := range []string{"session:first", "session:second"} {
		write := CacheCommandRequest{Command: "SETSTR", Key: key, Value: "value"}
		response := trie.ExecuteCommand(write)
		if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
			t.Fatalf("enqueue %s result = %#v, want queued", key, result)
		}
	}
	select {
	case key := <-attempted:
		if key != "session:first" {
			t.Fatalf("first failed attempt key = %q, want session:first", key)
		}
	case <-time.After(time.Second):
		t.Fatal("first replication attempt did not start")
	}
	replicator.Close()
	if err := outbox.Close(); err != nil {
		t.Fatalf("Close(outbox) error = %v", err)
	}

	reopened, err := OpenLevelDBReplicationOutbox(outboxPath)
	if err != nil {
		t.Fatalf("OpenLevelDBReplicationOutbox(reopen) error = %v", err)
	}
	if jobs := reopened.jobs(); len(jobs) != 2 || jobs[0].result.Key != "session:first" || jobs[1].result.Key != "session:second" {
		t.Fatalf("persisted jobs = %#v, want first then second", jobs)
	}
	mode.Store(1)
	replayed := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 2,
		AsyncOutbox:    reopened,
	})
	defer reopened.Close()
	defer replayed.Close()

	for _, want := range []string{"session:first", "session:second"} {
		select {
		case got := <-delivered:
			if got != want {
				t.Fatalf("delivered key = %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for replayed %s", want)
		}
	}
	waitUntil(t, time.Second, func() bool {
		return len(reopened.jobs()) == 0
	})
}

func TestHTTPReplicatorCircuitBreakerSkipsOnlyOpenTarget(t *testing.T) {
	failedRequests := make(chan struct{}, 4)
	healthyRequests := make(chan struct{}, 4)
	failingTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case failedRequests <- struct{}{}:
		default:
		}
		http.Error(w, "still down", http.StatusBadGateway)
	}))
	defer failingTarget.Close()
	healthyTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case healthyRequests <- struct{}{}:
		default:
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer healthyTarget.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopologyWithReplicas(t, failingTarget.URL, healthyTarget.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                   "node-a",
		Topology:               topology,
		Election:               election,
		Client:                 failingTarget.Client(),
		CircuitBreakerFailures: 1,
		CircuitBreakerCooldown: time.Hour,
	})

	firstWrite := CacheCommandRequest{Command: "SETSTR", Key: "session:breaker", Value: "first"}
	first := replicator.ReplicateCommand(context.Background(), trie, firstWrite, trie.ExecuteCommand(firstWrite))
	if len(first.Targets) != 2 || first.Targets[0].Node != "node-b" || !first.Targets[0].CircuitOpen || first.Targets[1].Node != "node-c" || !first.Targets[1].OK {
		t.Fatalf("first replication targets = %#v, want node-b open and node-c ok", first.Targets)
	}
	if len(failedRequests) != 1 || len(healthyRequests) != 1 {
		t.Fatalf("request counts after first write = failed:%d healthy:%d, want 1/1", len(failedRequests), len(healthyRequests))
	}

	secondWrite := CacheCommandRequest{Command: "SETSTR", Key: "session:breaker", Value: "second"}
	second := replicator.ReplicateCommand(context.Background(), trie, secondWrite, trie.ExecuteCommand(secondWrite))
	if len(second.Targets) != 2 || second.Targets[0].Node != "node-b" || !second.Targets[0].CircuitOpen || second.Targets[0].CircuitState != replicationCircuitOpen || second.Targets[1].Node != "node-c" || !second.Targets[1].OK {
		t.Fatalf("second replication targets = %#v, want node-b skipped open and node-c ok", second.Targets)
	}
	if len(failedRequests) != 1 || len(healthyRequests) != 2 {
		t.Fatalf("request counts after open breaker = failed:%d healthy:%d, want 1/2", len(failedRequests), len(healthyRequests))
	}
	last := replicator.LastResult()
	if len(last.CircuitBreakers) != 1 || last.CircuitBreakers[0].Node != "node-b" || last.CircuitBreakers[0].State != replicationCircuitOpen {
		t.Fatalf("circuit breakers = %#v, want only node-b open", last.CircuitBreakers)
	}
	if last.Health == "ok" || !strings.Contains(last.HealthReason, "circuit breaker") {
		t.Fatalf("health = %s/%d/%q, want breaker-degraded health", last.Health, last.HealthScore, last.HealthReason)
	}
}

func TestHTTPReplicatorCircuitBreakerHalfOpenRecovers(t *testing.T) {
	var attempts atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			http.Error(w, "try later", http.StatusBadGateway)
			return
		}
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                   "node-a",
		Topology:               topology,
		Election:               election,
		Client:                 target.Client(),
		CircuitBreakerFailures: 1,
		CircuitBreakerCooldown: 10 * time.Millisecond,
	})

	firstWrite := CacheCommandRequest{Command: "SETSTR", Key: "session:recover", Value: "first"}
	first := replicator.ReplicateCommand(context.Background(), trie, firstWrite, trie.ExecuteCommand(firstWrite))
	if len(first.Targets) != 1 || !first.Targets[0].CircuitOpen {
		t.Fatalf("first targets = %#v, want open circuit after failure", first.Targets)
	}
	secondWrite := CacheCommandRequest{Command: "SETSTR", Key: "session:recover", Value: "second"}
	second := replicator.ReplicateCommand(context.Background(), trie, secondWrite, trie.ExecuteCommand(secondWrite))
	if len(second.Targets) != 1 || !second.Targets[0].CircuitOpen || attempts.Load() != 1 {
		t.Fatalf("second targets = %#v attempts=%d, want skipped while open", second.Targets, attempts.Load())
	}

	time.Sleep(20 * time.Millisecond)
	thirdWrite := CacheCommandRequest{Command: "SETSTR", Key: "session:recover", Value: "third"}
	third := replicator.ReplicateCommand(context.Background(), trie, thirdWrite, trie.ExecuteCommand(thirdWrite))
	if len(third.Targets) != 1 || !third.Targets[0].OK || third.Targets[0].CircuitState != replicationCircuitClosed {
		t.Fatalf("third targets = %#v, want half-open probe success and closed circuit", third.Targets)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want one failure plus one half-open probe", attempts.Load())
	}
	if breakers := replicator.LastResult().CircuitBreakers; len(breakers) != 0 {
		t.Fatalf("circuit breakers = %#v, want cleared after successful probe", breakers)
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
	if last.Queue.OldestQueuedAt == nil || last.Queue.OldestQueuedKey != "session:2" || last.Queue.OldestQueuedAgeMillis < 0 {
		t.Fatalf("oldest queued stats = %#v, want pending session:2 with age", last.Queue)
	}
	if last.Queue.InFlightStartedAt == nil || last.Queue.InFlightKey != "session:1" || last.Queue.InFlightAgeMillis < 0 {
		t.Fatalf("in-flight stats = %#v, want blocked session:1 with age", last.Queue)
	}
	if last.Queue.DroppedByTarget["node-b"] != 1 {
		t.Fatalf("dropped target stats = %#v, want one dropped node-b target", last.Queue)
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

func TestHTTPReplicatorAsyncContextCancelStopsQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Context:        ctx,
		AsyncQueueSize: 1,
	})
	cancel()
	waitUntil(t, time.Second, func() bool {
		last := replicator.LastResult()
		return last.Queue != nil && last.Queue.Closed
	})

	trie := newTestTrie(t)
	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}
	response := trie.ExecuteCommand(write)
	result := replicator.ReplicateCommand(context.Background(), trie, write, response)
	if !result.Skipped || result.Reason != "replication queue is closed" || result.Queued {
		t.Fatalf("post-cancel replicate result = %#v, want closed queue skip", result)
	}
	last := replicator.LastResult()
	if last.Queue == nil || !last.Queue.Closed || last.Queue.Dropped != 1 {
		t.Fatalf("closed queue stats after context cancel = %#v, want closed with one dropped enqueue", last.Queue)
	}

	stopped := make(chan struct{})
	go func() {
		replicator.Close()
		replicator.Close()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("async replicator Close did not return after context cancel")
	}
}

func TestHTTPReplicatorAsyncCloseCancelsInFlightDelivery(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
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
		Timeout:        time.Minute,
	})

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("async replication target was not called")
	}

	stopped := make(chan struct{})
	go func() {
		replicator.Close()
		replicator.Close()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("async replicator Close did not cancel in-flight delivery")
	}
	last := replicator.LastResult()
	if last.Queue == nil || !last.Queue.Closed {
		t.Fatalf("queue stats after canceled close = %#v, want closed queue", last.Queue)
	}
}

func TestHTTPReplicatorAsyncCloseCancelsRetryWait(t *testing.T) {
	requests := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requests <- struct{}{}:
		default:
		}
		http.Error(w, "retry me", http.StatusBadGateway)
	}))
	defer target.Close()

	trie := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:               "node-a",
		Topology:           topology,
		Election:           election,
		Client:             target.Client(),
		AsyncQueueSize:     1,
		AsyncMaxAttempts:   2,
		AsyncRetryInterval: time.Hour,
	})

	write := CacheCommandRequest{Command: "SETSTR", Key: "session:1", Value: "value"}
	response := trie.ExecuteCommand(write)
	if result := replicator.ReplicateCommand(context.Background(), trie, write, response); !result.Queued || result.Skipped {
		t.Fatalf("enqueue result = %#v, want queued", result)
	}
	select {
	case <-requests:
	case <-time.After(time.Second):
		t.Fatal("async replication target was not called")
	}
	waitUntil(t, time.Second, func() bool {
		last := replicator.LastResult()
		return last.Queue != nil && last.Queue.Retried == 1
	})

	stopped := make(chan struct{})
	go func() {
		replicator.Close()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("async replicator Close did not cancel retry wait")
	}
	last := replicator.LastResult()
	if last.Queue == nil || !last.Queue.Closed || last.Queue.Retried != 1 || last.Queue.Attempts != 1 {
		t.Fatalf("queue stats after canceled retry wait = %#v, want one retry wait canceled", last.Queue)
	}
}

func TestHTTPReplicatorUsesTopologyWhenElectionUnconfigured(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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
	if result.Skipped || result.Command != "SYNC" || result.Key != "session:" || result.Entries != 2 || len(result.Targets) != 1 {
		t.Fatalf("sync result = %#v, want one batched target for two synced entries", result)
	}
	assertReplicationResultTiming(t, result)
	for _, target := range result.Targets {
		if !target.OK || target.Node != "node-b" {
			t.Fatalf("sync target = %#v, want ok node-b target", target)
		}
	}
	request := <-requests
	payloads := mustDecodeReplicationBatchValues(t, request)
	if len(payloads) != 2 {
		t.Fatalf("sync batch payloads len = %d, want 2: %#v", len(payloads), payloads)
	}
	targetKeys := map[string]bool{}
	for _, payload := range payloads {
		if payload.Command != "INTERNALSET" || payload.Value == "" {
			t.Fatalf("sync batch payload = %#v, want INTERNALSET snapshot", payload)
		}
		targetKeys[payload.Key] = true
	}
	if !targetKeys["session:1"] || !targetKeys["session:2"] {
		t.Fatalf("sync batch keys = %#v, want session keys", targetKeys)
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

func TestHTTPReplicatorRecordsLatencyAndBatchMetrics(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = mustDecodeReplicationTestCommand(t, w, r)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value-1")
	trie.UpsertString("session:2", "value-2")
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	result := replicator.SyncAll(context.Background(), trie, "session:")
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("sync result = %#v, want one ok target", result)
	}
	metrics := replicator.MetricsSnapshot()
	latency := metrics.TargetLatencyMillis["node-b"]
	if latency.Count != 1 || latency.Sum < 0 {
		t.Fatalf("target latency histogram = %#v, want one observation", latency)
	}
	batch := metrics.TargetBatchItems["node-b"]
	if batch.Count != 1 || batch.Sum != 2 {
		t.Fatalf("batch size histogram = %#v, want one two-item observation", batch)
	}
}

func TestHTTPReplicatorSplitsSyncBatchesByEstimatedBytes(t *testing.T) {
	requests := make(chan CacheCommandRequest, 3)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		requests <- mustDecodeReplicationTestCommand(t, w, r)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value-1")
	trie.UpsertString("session:2", "value-2")
	trie.UpsertString("session:3", "value-3")
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                     "node-a",
		Topology:                 topology,
		Election:                 election,
		Client:                   target.Client(),
		ReplicationBatchMaxBytes: 1,
	})

	result := replicator.SyncAll(context.Background(), trie, "session:")
	if result.Skipped || result.Entries != 3 || len(result.Targets) != 3 {
		t.Fatalf("sync result = %#v, want three split target requests", result)
	}
	seen := map[string]bool{}
	for idx := 0; idx < 3; idx++ {
		request := <-requests
		if request.Command != "INTERNALSET" || request.Value == "" {
			t.Fatalf("split request %d = %#v, want single INTERNALSET snapshot", idx, request)
		}
		seen[request.Key] = true
	}
	for _, key := range []string{"session:1", "session:2", "session:3"} {
		if !seen[key] {
			t.Fatalf("split requests keys = %#v, missing %s", seen, key)
		}
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected sync request = %#v", request)
	default:
	}
}

func TestGroupReplicationTasksByTargetAdaptiveMatchesMapGrouping(t *testing.T) {
	for _, tt := range []struct {
		name          string
		uniqueTargets int
		tasks         int
	}{
		{name: "linear path", uniqueTargets: 3, tasks: 12},
		{name: "map fallback", uniqueTargets: 8, tasks: 16},
		{name: "large map path", uniqueTargets: 64, tasks: 128},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tasks := replicationGroupingBenchmarkTasks(tt.uniqueTargets, tt.tasks)
			got := groupReplicationTasksByTarget(tasks)
			want := groupReplicationTasksByTargetMap(tasks)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("adaptive groups = %#v, want map groups %#v", got, want)
			}
		})
	}
}

func TestHTTPReplicatorSyncAllFullReplicaReplicatesToRemoteOwners(t *testing.T) {
	type targetRequest struct {
		node    string
		request CacheCommandRequest
	}
	requests := make(chan targetRequest, 2)
	newTarget := func(node string) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/commands" {
				t.Fatalf("path = %s, want /api/commands", r.URL.Path)
			}
			requests <- targetRequest{
				node:    node,
				request: mustDecodeReplicationTestCommand(t, w, r),
			}
			writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
		}))
	}
	targetB := newTarget("node-b")
	defer targetB.Close()
	targetC := newTarget("node-c")
	defer targetC.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value-1")
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Mode:    TopologyModeFullReplica,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://127.0.0.1:1"},
			{ID: "node-b", Address: targetB.URL},
			{ID: "node-c", Address: targetC.URL},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore(full replica) error = %v", err)
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Client:   targetB.Client(),
	})

	result := replicator.SyncAll(context.Background(), trie, "session:")
	if result.Skipped || result.Command != "SYNC" || result.Key != "session:" || result.Entries != 1 || len(result.Targets) != 2 {
		t.Fatalf("full-replica sync result = %#v, want one entry replicated to two owners", result)
	}
	if result.Targets[0].Node != "node-b" || result.Targets[1].Node != "node-c" {
		t.Fatalf("full-replica sync targets = %#v, want node-b then node-c", result.Targets)
	}
	for _, target := range result.Targets {
		if !target.OK || target.Key != "session:1" || target.Node == "node-a" {
			t.Fatalf("full-replica sync target = %#v, want ok remote owner for session:1", target)
		}
	}

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		targetRequest := <-requests
		if targetRequest.request.Command != "INTERNALSET" || targetRequest.request.Key != "session:1" || targetRequest.request.Value == "" {
			t.Fatalf("full-replica sync request = %#v, want INTERNALSET session snapshot", targetRequest.request)
		}
		seen[targetRequest.node] = true
	}
	if !seen["node-b"] || !seen["node-c"] || seen["node-a"] {
		t.Fatalf("full-replica sync request nodes = %#v, want only remote owners", seen)
	}
	select {
	case targetRequest := <-requests:
		t.Fatalf("unexpected full-replica sync request = %#v", targetRequest)
	default:
	}
}

func TestHTTPReplicatorSyncAllPagesLeaderOwnedEntries(t *testing.T) {
	requests := make(chan CacheCommandRequest, 3)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	trie.UpsertString("session:1", "value-1")
	trie.UpsertString("session:2", "value-2")
	trie.UpsertString("session:3", "value-3")
	trie.UpsertString("other:1", "ignored")
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	result := replicator.syncAllPaged(context.Background(), trie, "session:", 2)
	if result.Skipped || result.Entries != 3 || len(result.Targets) != 2 {
		t.Fatalf("paged sync result = %#v, want two target requests for three synced entries", result)
	}
	for _, target := range result.Targets {
		if !target.OK {
			t.Fatalf("paged sync target = %#v, want ok target", target)
		}
	}
	targetKeys := map[string]bool{}
	batches := 0
	singles := 0
	for i := 0; i < 2; i++ {
		request := <-requests
		switch request.Command {
		case "INTERNALBATCH":
			batches++
			payloads := mustDecodeReplicationBatchValues(t, request)
			if len(payloads) != 2 {
				t.Fatalf("paged sync batch len = %d, want 2: %#v", len(payloads), payloads)
			}
		case "INTERNALSET":
			singles++
			if request.Value == "" {
				t.Fatalf("paged sync single request = %#v, want snapshot value", request)
			}
		default:
			t.Fatalf("paged sync request = %#v, want internal replication request", request)
		}
		for _, key := range replicationRequestKeys(t, request) {
			if !strings.HasPrefix(key, "session:") {
				t.Fatalf("paged sync request key = %q, want session key", key)
			}
			targetKeys[key] = true
		}
	}
	if batches != 1 || singles != 1 {
		t.Fatalf("paged sync request shape = %d batches %d singles, want 1 batch and 1 single", batches, singles)
	}
	for _, key := range []string{"session:1", "session:2", "session:3"} {
		if !targetKeys[key] {
			t.Fatalf("paged sync request keys = %#v, missing %s", targetKeys, key)
		}
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected paged sync request = %#v", request)
	default:
	}
}

func TestReplicationSyncKeysPageAdvancesAfterEmptyKey(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("", "empty")
	trie.UpsertString("session:1", "value")

	page, err := replicationSyncKeysPage(trie, "", "", false, 1)
	if err != nil {
		t.Fatalf("replicationSyncKeysPage(first) error = %v", err)
	}
	if !reflect.DeepEqual(page.keys, []string{""}) || !page.hasMore || page.nextAfterKey != "" {
		t.Fatalf("first page = %#v, want empty key with more entries", page)
	}

	page, err = replicationSyncKeysPage(trie, "", page.nextAfterKey, true, 1)
	if err != nil {
		t.Fatalf("replicationSyncKeysPage(second) error = %v", err)
	}
	if !reflect.DeepEqual(page.keys, []string{"session:1"}) || page.hasMore || page.nextAfterKey != "session:1" {
		t.Fatalf("second page = %#v, want session key without more entries", page)
	}
}

func TestHTTPReplicatorSyncAllSkipsExpiredEntries(t *testing.T) {
	requests := make(chan CacheCommandRequest, 2)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	trie := newTestTrie(t)
	base := time.Unix(800, 0)
	trie.now = func() time.Time { return base }
	trie.UpsertString("session:expired", "old")
	trie.UpsertString("session:live", "new")
	if !trie.Expire("session:expired", time.Second) {
		t.Fatal("Expire(session:expired) = false, want true")
	}
	if !trie.Expire("session:live", 10*time.Second) {
		t.Fatal("Expire(session:live) = false, want true")
	}
	trie.now = func() time.Time { return base.Add(2 * time.Second) }

	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})

	result := replicator.SyncAll(context.Background(), trie, "session:")
	if result.Skipped || result.Entries != 1 || len(result.Targets) != 1 || result.Targets[0].Key != "session:live" {
		t.Fatalf("sync result = %#v, want only live session entry", result)
	}
	request := <-requests
	if request.Command != "INTERNALSET" || request.Key != "session:live" {
		t.Fatalf("sync request = %#v, want live INTERNALSET", request)
	}
	select {
	case request := <-requests:
		t.Fatalf("unexpected sync request = %#v", request)
	default:
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

func TestPostReplicationCommandRejectsOversizedResponseBody(t *testing.T) {
	body := `{"ok":true,"message":"` + strings.Repeat("x", maxHTTPReplicationResponseBytes) + `"}`
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    request,
			}, nil
		}),
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: client})

	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: "127.0.0.1:8080",
	}, CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: "{}"})
	if result.OK || result.Status != http.StatusOK || !strings.Contains(result.Error, "replication response is too large") {
		t.Fatalf("postReplicationCommand() = %#v, want oversized response error", result)
	}
}

func TestDecodeReplicationCommandResponseRejectsOversizedTrailingWhitespace(t *testing.T) {
	body := `{"ok":true,"message":"ok"}` + strings.Repeat(" ", maxHTTPReplicationResponseBytes+1)
	_, err := decodeReplicationCommandResponse(strings.NewReader(body))
	if !errors.Is(err, errReplicationResponseTooLarge) {
		t.Fatalf("decodeReplicationCommandResponse() error = %v, want response too large", err)
	}
}

func TestPostReplicationCommandRejectsTrailingResponseJSON(t *testing.T) {
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"ok":true,"message":"ok"}{"ok":true,"message":"ok"}`)),
				Request:    request,
			}, nil
		}),
	}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Client: client})

	result := replicator.postReplicationCommand(context.Background(), TopologyNode{
		ID:      "node-b",
		Address: "127.0.0.1:8080",
	}, CacheCommandRequest{Command: "INTERNALSET", Key: "session:1", Value: "{}"})
	if result.OK || result.Status != http.StatusOK || !strings.Contains(result.Error, "invalid command response JSON") {
		t.Fatalf("postReplicationCommand() = %#v, want trailing JSON error", result)
	}
}

func TestHTTPReplicatorCompressesLargeReplicationRequests(t *testing.T) {
	follower := newTestTrie(t)
	var contentEncoding string
	handler := NewMonitoringHandler(follower, MonitoringOptions{}).Handler()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentEncoding = r.Header.Get("Content-Encoding")
		handler.ServeHTTP(w, r)
	}))
	defer target.Close()

	leader := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})
	large := strings.Repeat("x", minCompressedReplicationRequestBytes)
	write := CacheCommandRequest{Command: "SETSTR", Key: "session:large", Value: large}
	response := leader.ExecuteCommand(write)
	if !response.OK {
		t.Fatalf("leader SETSTR response = %#v, want ok", response)
	}

	result := replicator.ReplicateCommand(context.Background(), leader, write, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("large replication result = %#v, want one ok target", result)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("replication Content-Encoding = %q, want gzip", contentEncoding)
	}
	if got := follower.GetString("session:large"); got != large {
		t.Fatalf("follower value length = %d, want %d", len(got), len(large))
	}
}

func TestReplicationRequestBodyStreamsLargeStringPayload(t *testing.T) {
	payload := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:large",
		Value:   strings.Repeat("x", minCompressedReplicationRequestBytes),
	}
	body, contentEncoding, err := replicationRequestBody(payload)
	if err != nil {
		t.Fatalf("replicationRequestBody() error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}
	if _, ok := body.(*jsonwire.StreamingGzipJSONBody); !ok {
		t.Fatalf("replicationRequestBody() body = %T, want streaming gzip body", body)
	}

	compressed, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll(compressed body) error = %v", err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("NewReader(compressed body) error = %v", err)
	}
	defer reader.Close()

	var decoded CacheCommandRequest
	if err := json.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(compressed body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded payload = %#v, want %#v", decoded, payload)
	}
}

func TestEstimatedReplicationRequestBytesUsesExactOptionalIntSize(t *testing.T) {
	priority := int64(3)
	ttl := int64(60)
	unix := int64(-7)
	base := CacheCommandRequest{Command: "INTERNALSET", Key: "session:ttl", Value: "value"}
	withOptionals := base
	withOptionals.Priority = &priority
	withOptionals.TTLSeconds = &ttl
	withOptionals.UnixSeconds = &unix

	wantExtra := jsonwire.EstimateJSONValueBytes(priority, minCompressedReplicationRequestBytes) +
		jsonwire.EstimateJSONValueBytes(ttl, minCompressedReplicationRequestBytes) +
		jsonwire.EstimateJSONValueBytes(unix, minCompressedReplicationRequestBytes)
	gotExtra := estimatedReplicationRequestBytes(withOptionals) - estimatedReplicationRequestBytes(base)
	if gotExtra != wantExtra {
		t.Fatalf("estimated optional int bytes = %d, want exact numeric bytes %d", gotExtra, wantExtra)
	}
	if got := addEstimatedOptionalCommandInt64(minCompressedReplicationRequestBytes-1, &priority, minCompressedReplicationRequestBytes); got != minCompressedReplicationRequestBytes {
		t.Fatalf("addEstimatedOptionalCommandInt64(capped) = %d, want threshold", got)
	}
}

func TestReplicationRequestBodyStreamsEscapedLargeStringPayload(t *testing.T) {
	payload := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:escaped",
		Value:   strings.Repeat("\n", minCompressedReplicationRequestBytes/2),
	}
	body, contentEncoding, err := replicationRequestBody(payload)
	if err != nil {
		t.Fatalf("replicationRequestBody(escaped value) error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}
	if _, ok := body.(*jsonwire.StreamingGzipJSONBody); !ok {
		t.Fatalf("replicationRequestBody(escaped value) body = %T, want streaming gzip body", body)
	}

	reader, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("NewReader(streaming gzip body) error = %v", err)
	}
	defer reader.Close()

	var decoded CacheCommandRequest
	if err := json.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(streaming escaped body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded escaped payload = %#v, want %#v", decoded, payload)
	}
}

func TestReplicationRequestBodyLeavesSmallPayloadPlain(t *testing.T) {
	payload := CacheCommandRequest{
		Command: "INTERNALDEL",
		Key:     "session:small",
	}
	body, contentEncoding, err := replicationRequestBody(payload)
	if err != nil {
		t.Fatalf("replicationRequestBody() error = %v", err)
	}
	if contentEncoding != "" {
		t.Fatalf("Content-Encoding = %q, want empty", contentEncoding)
	}

	var decoded CacheCommandRequest
	if err := json.NewDecoder(body).Decode(&decoded); err != nil {
		t.Fatalf("Decode(plain body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded plain payload = %#v, want %#v", decoded, payload)
	}
}

func TestReplicationRequestBodyStreamsLargeStructuredPayload(t *testing.T) {
	values := make(Slice, 0, minCompressedReplicationRequestBytes/4)
	for len(values) < cap(values) {
		values = append(values, strings.Repeat("value", 4))
	}
	payload := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:structured",
		Values:  values,
	}
	body, contentEncoding, err := replicationRequestBody(payload)
	if err != nil {
		t.Fatalf("replicationRequestBody() error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}
	if _, ok := body.(*jsonwire.StreamingGzipJSONBody); !ok {
		t.Fatalf("replicationRequestBody() body = %T, want streaming gzip body", body)
	}

	reader, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("NewReader(streaming gzip body) error = %v", err)
	}
	defer reader.Close()

	var decoded CacheCommandRequest
	if err := json.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(streaming gzip body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded streaming payload = %#v, want %#v", decoded, payload)
	}
}

func TestReplicationRequestBodyStreamsLargePairPayload(t *testing.T) {
	pairs := make(Map, minCompressedReplicationRequestBytes/128)
	for len(pairs) < minCompressedReplicationRequestBytes/128 {
		key := strings.Repeat("profile:", 2) + string(rune('a'+len(pairs)/26)) + string(rune('a'+len(pairs)%26))
		pairs[key] = strings.Repeat("value", 32)
	}
	payload := CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:pairs",
		Pairs:   pairs,
	}
	body, contentEncoding, err := replicationRequestBody(payload)
	if err != nil {
		t.Fatalf("replicationRequestBody() error = %v", err)
	}
	if contentEncoding != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", contentEncoding)
	}
	if _, ok := body.(*jsonwire.StreamingGzipJSONBody); !ok {
		t.Fatalf("replicationRequestBody() body = %T, want streaming gzip body", body)
	}

	reader, err := gzip.NewReader(body)
	if err != nil {
		t.Fatalf("NewReader(streaming gzip body) error = %v", err)
	}
	defer reader.Close()

	var decoded CacheCommandRequest
	if err := json.NewDecoder(reader).Decode(&decoded); err != nil {
		t.Fatalf("Decode(streaming gzip body) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Fatalf("decoded streaming payload = %#v, want %#v", decoded, payload)
	}
}

func TestReplicationRequestBodyReportsMarshalErrors(t *testing.T) {
	body, contentEncoding, err := replicationRequestBody(CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:bad",
		Values:  Slice{func() {}},
	})
	if err == nil {
		t.Fatal("replicationRequestBody(unsupported) error = nil, want error")
	}
	if body != nil {
		t.Fatalf("replicationRequestBody(unsupported) body = %T, want nil", body)
	}
	if contentEncoding != "" {
		t.Fatalf("Content-Encoding = %q, want empty on marshal error", contentEncoding)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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

func TestHTTPReplicatorReplicatesRadixTreeMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
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

	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "CREATERT", Key: "index"}); !response.OK {
		t.Fatalf("CREATERT response = %#v, want ok", response)
	}
	put := CacheCommandRequest{Command: "PUTRT", Key: "index", Subkey: "user:100/profile", Value: "active"}
	response := trie.ExecuteCommand(put)
	if !response.OK {
		t.Fatalf("PUTRT response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, put, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("radix tree replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "index" || request.Value == "" {
			t.Fatalf("replicated radix request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated radix snapshot JSON error = %v", err)
		}
		if entry.Type != "radix_tree" || entry.RadixTree == nil || entry.RadixTree.Count != 1 {
			t.Fatalf("replicated radix snapshot = %#v, want radix_tree payload", entry)
		}
	default:
		t.Fatal("radix tree mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASRT", Key: "index", Subkey: "user:100/profile"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASRT", Key: "index", Subkey: "user:100/profile"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("radix tree read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesCuckooFilterMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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

func TestHTTPReplicatorReplicatesSparseBitsetMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
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

	create := CacheCommandRequest{Command: "CREATESB", Key: "ids"}
	if response := trie.ExecuteCommand(create); !response.OK {
		t.Fatalf("CREATESB response = %#v, want ok", response)
	}
	add := CacheCommandRequest{Command: "ADDSB", Key: "ids", Values: Slice{json.Number("1"), json.Number("65543"), json.Number("18446744073709551615")}}
	response := trie.ExecuteCommand(add)
	if !response.OK {
		t.Fatalf("ADDSB response = %#v, want ok", response)
	}
	result := replicator.ReplicateCommand(context.Background(), trie, add, response)
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("sparse bitset replication result = %#v, want one ok target", result)
	}

	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "ids" || request.Value == "" {
			t.Fatalf("replicated sparse bitset request = %#v, want INTERNALSET snapshot", request)
		}
		var entry snapshotEntry
		if err := json.Unmarshal([]byte(request.Value), &entry); err != nil {
			t.Fatalf("replicated sparse bitset snapshot JSON error = %v", err)
		}
		if entry.Type != "sparse_bitset" || entry.SparseBitset == nil {
			t.Fatalf("replicated sparse bitset snapshot = %#v, want sparse_bitset payload", entry)
		}
	default:
		t.Fatal("sparse bitset mutation did not reach remote target")
	}

	read := trie.ExecuteCommand(CacheCommandRequest{Command: "HASSB", Key: "ids", Value: "1"})
	result = replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{Command: "HASSB", Key: "ids", Value: "1"}, read)
	if !result.Skipped || result.Reason != "command is not replicated" {
		t.Fatalf("sparse bitset read replication result = %#v, want skipped read command", result)
	}
}

func TestHTTPReplicatorReplicatesCountMinSketchMutations(t *testing.T) {
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mustDecodeReplicationTestCommand(t, w, r)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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
		request := mustDecodeReplicationTestCommand(t, w, r)
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

func replicationTestTopologyWithReplicas(t *testing.T, replicaBAddress string, replicaCAddress string) *TopologyStore {
	t.Helper()
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://127.0.0.1:1"},
			{ID: "node-b", Address: replicaBAddress},
			{ID: "node-c", Address: replicaCAddress},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b", "node-c"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	return topology
}
