package hatCache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestTR004ReplicationKeyPrefixesAreCopiedAndLiteral(t *testing.T) {
	prefixes := []string{"region:eu:", "tenant/"}
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{ReplicationKeyPrefixes: prefixes})
	prefixes[0] = "region:us:"

	if !replicator.replicationKeyAllowed("region:eu:orders") {
		t.Fatal("configured region prefix was changed through caller slice")
	}
	if replicator.replicationKeyAllowed("region:us:orders") {
		t.Fatal("unconfigured region prefix was allowed")
	}
	if !replicator.replicationKeyAllowed("tenant/42") {
		t.Fatal("configured tenant prefix was rejected")
	}
	if replicator.replicationKeyAllowed("region:eu") {
		t.Fatal("prefix match was not literal")
	}
}

func TestTR004ReplicationKeyPrefixesDefaultDisabled(t *testing.T) {
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{})
	if !replicator.replicationKeyAllowed("any:key") {
		t.Fatal("default replication key filter is enabled")
	}
}

func TestTR004ReplicationKeyPrefixesFilterLivePlanner(t *testing.T) {
	topology := replicationTestTopology(t, "http://node-b")
	replicator := &HTTPReplicator{
		self:        "node-a",
		topology:    topology,
		keyPrefixes: []string{"region:eu:"},
	}
	result, _, targets, ok := replicator.planLiveReplicationTargets(context.Background(), CacheCommandRequest{
		Command: "SETSTR",
		Key:     "region:us:orders",
	}, CacheCommandResponse{OK: true})
	if ok || len(targets.multiple) != 0 || targets.single.ID != "" || !result.Skipped || result.Reason != replicationKeyPrefixExcludedReason {
		t.Fatalf("filtered live planner = %#v/%#v/%v, want prefix-filter skip", result, targets, ok)
	}
}

func TestTR004ReplicationKeyPrefixesWireRoundTrip(t *testing.T) {
	want := []string{"region:eu:", "tenant/", "literal space"}
	encoded := encodeReplicationKeyPrefixes(want)
	got, err := decodeReplicationKeyPrefixes(encoded)
	if err != nil {
		t.Fatalf("decodeReplicationKeyPrefixes() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded prefixes = %#v, want %#v", got, want)
	}
}

func TestTR004ReplicationKeyPrefixesRejectMalformedWire(t *testing.T) {
	for _, encoded := range []string{"not-base64", "Ag==", "AQECAQ"} {
		if _, err := decodeReplicationKeyPrefixes(encoded); err == nil {
			t.Fatalf("decodeReplicationKeyPrefixes(%q) error = nil, want validation error", encoded)
		}
	}
}

func TestTR004ReplicationKeyPrefixesFilterLiveAndDigestSync(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("region:eu:1", "eu")
	source.UpsertString("region:us:1", "us")

	targetTrie := newTestTrie(t)
	targetTrie.UpsertString("region:eu:stale", "stale")
	targetTrie.UpsertString("region:us:preserved", "keep")

	var targetHandler http.Handler
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetHandler.ServeHTTP(writer, request)
	}))
	defer target.Close()
	topology := replicationTestTopology(t, target.URL)
	targetHandler = NewMonitoringHandler(targetTrie, MonitoringOptions{
		NodeName:          "node-b",
		Topology:          topology,
		ReplicationSafety: NewReplicationSafetyStore(),
	}).Handler()
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                   "node-a",
		Topology:               topology,
		Election:               NewElectionStore(topology, ElectionOptions{}),
		Client:                 target.Client(),
		ReplicationKeyPrefixes: []string{"region:eu:"},
	})
	defer replicator.Close()

	liveRequest := CacheCommandRequest{Command: "SETSTR", Key: "region:eu:live", Value: "live"}
	source.UpsertString(liveRequest.Key, liveRequest.Value)
	liveResult := replicator.ReplicateCommand(context.Background(), source, liveRequest, CacheCommandResponse{OK: true})
	if liveResult.Skipped || len(liveResult.Targets) != 1 || !liveResult.Targets[0].OK {
		t.Fatalf("filtered live result = %#v, want one successful target", liveResult)
	}

	excludedRequest := CacheCommandRequest{Command: "SETSTR", Key: "region:us:live", Value: "must-not-replicate"}
	source.UpsertString(excludedRequest.Key, excludedRequest.Value)
	excludedResult := replicator.ReplicateCommand(context.Background(), source, excludedRequest, CacheCommandResponse{OK: true})
	if !excludedResult.Skipped || excludedResult.Reason != replicationKeyPrefixExcludedReason {
		t.Fatalf("excluded live result = %#v, want prefix-filter skip", excludedResult)
	}

	syncResult := replicator.SyncAll(context.Background(), source, "region:")
	if syncResult.Skipped || syncResult.Entries != 2 || len(syncResult.Targets) != 1 || !syncResult.Targets[0].OK {
		t.Fatalf("filtered digest result = %#v, want two selected entries and one successful target", syncResult)
	}
	if targetTrie.GetString("region:eu:1") != "eu" || targetTrie.GetString("region:eu:live") != "live" {
		t.Fatalf("selected target values = %q/%q, want source values", targetTrie.GetString("region:eu:1"), targetTrie.GetString("region:eu:live"))
	}
	if targetTrie.Exists("region:eu:stale") {
		t.Fatal("stale key inside selected prefix was not deleted")
	}
	if targetTrie.GetString("region:us:preserved") != "keep" {
		t.Fatal("key outside selected prefix was deleted or changed")
	}
	if targetTrie.Exists("region:us:live") {
		t.Fatal("excluded live key was replicated")
	}
}

func TestTR004ReplicationKeyPrefixesPreserveLegacyPeerExtras(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("region:eu:1", "eu")
	targetTrie := newTestTrie(t)
	targetTrie.UpsertString("region:eu:stale", "stale")
	targetTrie.UpsertString("region:us:preserved", "keep")
	safety := NewReplicationSafetyStore()
	var topology *TopologyStore
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		decoded := mustDecodeReplicationTestCommand(t, writer, request)
		if normalizedCommand(decoded.Command) == replicationDigestCommand && decoded.Pairs != nil {
			delete(decoded.Pairs, replicationKeyPrefixesMetadata)
		}
		response, rejected := executeCacheCommand(request.Context(), targetTrie, decoded, commandExecutionOptions{
			NodeName:          "node-b",
			Topology:          topology,
			ReplicationSafety: safety,
		})
		status := http.StatusOK
		if rejected {
			status = http.StatusConflict
		}
		format, _ := commandWireFormatFromContentType(request.Header.Get("Content-Type"))
		writeCommandResponseWire(writer, request, status, response, format)
	}))
	defer target.Close()
	topology = replicationTestTopology(t, target.URL)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:                   "node-a",
		Topology:               topology,
		Election:               NewElectionStore(topology, ElectionOptions{}),
		Client:                 target.Client(),
		ReplicationKeyPrefixes: []string{"region:eu:"},
	})
	defer replicator.Close()

	result := replicator.SyncAll(context.Background(), source, "region:")
	if result.Skipped || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("legacy filtered digest result = %#v, want successful target", result)
	}
	if targetTrie.Exists("region:eu:stale") {
		t.Fatal("stale selected key was not deleted from legacy peer")
	}
	if targetTrie.GetString("region:us:preserved") != "keep" {
		t.Fatal("legacy peer extra outside selected prefix was deleted")
	}
}
