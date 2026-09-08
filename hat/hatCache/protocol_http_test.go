package hatCache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hatrie_cache/hat/hatCommand"
)

func TestMonitoringHTTPProtocolUsesConfiguredRange(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{
		ProtocolVersions: hatCommand.ProtocolVersionRange{Min: 1, Max: 2},
	}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"protocol:http","value":"value"}`))
	request.Header.Set(hatCommand.HeaderProtocolVersion, "1-2")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("command status = %d, want %d", response.Code, http.StatusOK)
	}
	if got, want := response.Header().Get(hatCommand.HeaderProtocolVersion), "2"; got != want {
		t.Fatalf("selected protocol header = %q, want %q", got, want)
	}
	if got, want := response.Header().Get(hatCommand.HeaderProtocolSupportedVersions), "1-2"; got != want {
		t.Fatalf("supported protocol header = %q, want %q", got, want)
	}
	if !strings.Contains(strings.Join(response.Header().Values("Vary"), ","), hatCommand.HeaderProtocolVersion) {
		t.Fatalf("Vary = %q, want protocol header", response.Header().Values("Vary"))
	}
}

func TestMonitoringHTTPProtocolRejectsIncompatibleRange(t *testing.T) {
	trie := newTestTrie(t)
	handler := NewMonitoringHandler(trie, MonitoringOptions{
		ProtocolVersions: hatCommand.ProtocolVersionRange{Min: 2, Max: 2},
	}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"protocol:rejected","value":"value"}`))
	request.Header.Set(hatCommand.HeaderProtocolVersion, "1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUpgradeRequired {
		t.Fatalf("incompatible command status = %d, want %d", response.Code, http.StatusUpgradeRequired)
	}
	if got := trie.GetString("protocol:rejected"); got != "" {
		t.Fatalf("incompatible command local write = %q, want no mutation", got)
	}
}

func TestHTTPReplicatorAdvertisesConfiguredProtocolRange(t *testing.T) {
	var gotHeader string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get(hatCommand.HeaderProtocolVersion)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "replicated"})
	}))
	defer target.Close()

	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:             "node-a",
		Topology:         topology,
		Election:         election,
		Client:           target.Client(),
		ProtocolVersions: hatCommand.ProtocolVersionRange{Min: 1, Max: 2},
	})
	t.Cleanup(replicator.Close)

	trie := newTestTrie(t)
	trie.UpsertString("protocol:replication", "value")
	result := replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{
		Command: "SETSTR", Key: "protocol:replication", Value: "value",
	}, CacheCommandResponse{OK: true})
	if len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("replication result = %#v, want one successful target", result)
	}
	if got, want := gotHeader, "1-2"; got != want {
		t.Fatalf("replication protocol header = %q, want %q", got, want)
	}
}

func TestHTTPReplicatorOmitsProtocolRangeByDefault(t *testing.T) {
	var gotHeader string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get(hatCommand.HeaderProtocolVersion)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "replicated"})
	}))
	defer target.Close()

	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})
	t.Cleanup(replicator.Close)

	trie := newTestTrie(t)
	trie.UpsertString("protocol:legacy", "value")
	replicator.ReplicateCommand(context.Background(), trie, CacheCommandRequest{
		Command: "SETSTR", Key: "protocol:legacy", Value: "value",
	}, CacheCommandResponse{OK: true})
	if gotHeader != "" {
		t.Fatalf("default replication protocol header = %q, want omitted", gotHeader)
	}
}

func BenchmarkMonitoringHTTPProtocolNegotiation(b *testing.B) {
	request := httptest.NewRequest(http.MethodPost, "/api/commands", nil)
	request.Header.Set(hatCommand.HeaderProtocolVersion, "1-2")
	server := hatCommand.ProtocolVersionRange{Min: 1, Max: 2}
	response := make(http.Header)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		response.Del(hatCommand.HeaderProtocolVersion)
		response.Del(hatCommand.HeaderProtocolSupportedVersions)
		response.Del("Vary")
		if _, err := hatCommand.NegotiateHTTPProtocol(request, response, server); err != nil {
			b.Fatal(err)
		}
	}
}
