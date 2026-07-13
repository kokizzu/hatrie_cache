package hatriecache

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestMonitoringHandlerExposesHealthStatsAndEntries(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(1000, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("session:1", "active user")
	ht.UpsertSet("session:tags", Set{"active", "paid"})
	ht.UpsertPriorityQueue("session:jobs", PriorityQueue{{Priority: 1, Value: "rebuild"}, {Priority: 5, Value: "compact"}})
	if err := ht.UpsertBloomFilter("session:seen", 1000, 0.001); err != nil {
		t.Fatalf("UpsertBloomFilter() error = %v", err)
	}
	ht.AddBloomFilter("session:seen", "email:1")
	if err := ht.UpsertCountMinSketch("session:freq", 128, 4); err != nil {
		t.Fatalf("UpsertCountMinSketch() error = %v", err)
	}
	ht.IncrementCountMinSketch("session:freq", "/api/users", 3)
	if err := ht.UpsertHyperLogLog("session:card", 10); err != nil {
		t.Fatalf("UpsertHyperLogLog() error = %v", err)
	}
	ht.AddHyperLogLog("session:card", "user:1", "user:2")
	if err := ht.UpsertTopK("session:top", 3); err != nil {
		t.Fatalf("UpsertTopK() error = %v", err)
	}
	ht.AddTopK("session:top", "/api/users", 7)
	ht.UpsertCounter("counter:views", 42)
	if !ht.Expire("session:1", time.Minute) {
		t.Fatal("Expire(session:1) = false, want true")
	}
	bloomInfo, ok := ht.BloomFilterInfo("session:seen")
	if !ok {
		t.Fatal("BloomFilterInfo(session:seen) = false, want true")
	}
	sketchInfo, ok := ht.CountMinSketchInfo("session:freq")
	if !ok {
		t.Fatal("CountMinSketchInfo(session:freq) = false, want true")
	}
	hllInfo, ok := ht.HyperLogLogInfo("session:card")
	if !ok {
		t.Fatal("HyperLogLogInfo(session:card) = false, want true")
	}
	topKInfo, ok := ht.TopKInfo("session:top")
	if !ok {
		t.Fatal("TopKInfo(session:top) = false, want true")
	}

	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName: "test-node",
		StartAt:  now.Add(-time.Hour),
	}).Handler()

	healthResp := httptest.NewRecorder()
	handler.ServeHTTP(healthResp, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	if healthResp.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", healthResp.Code)
	}
	var health MonitoringHealth
	if err := json.Unmarshal(healthResp.Body.Bytes(), &health); err != nil {
		t.Fatalf("health JSON error = %v", err)
	}
	if health.Node != "test-node" || health.Status != "online" {
		t.Fatalf("health = %#v, want test-node online", health)
	}

	statsResp := httptest.NewRecorder()
	handler.ServeHTTP(statsResp, httptest.NewRequest(http.MethodGet, "/api/stats", nil))
	if statsResp.Code != http.StatusOK {
		t.Fatalf("stats status = %d, want 200", statsResp.Code)
	}
	var stats CacheStats
	if err := json.Unmarshal(statsResp.Body.Bytes(), &stats); err != nil {
		t.Fatalf("stats JSON error = %v", err)
	}
	if stats.Writes == 0 {
		t.Fatalf("stats writes = 0, want existing cache writes")
	}

	entriesResp := httptest.NewRecorder()
	handler.ServeHTTP(entriesResp, httptest.NewRequest(http.MethodGet, "/api/entries?prefix=session:", nil))
	if entriesResp.Code != http.StatusOK {
		t.Fatalf("entries status = %d, want 200", entriesResp.Code)
	}
	var entries MonitoringEntriesResponse
	if err := json.Unmarshal(entriesResp.Body.Bytes(), &entries); err != nil {
		t.Fatalf("entries JSON error = %v", err)
	}
	if len(entries.Entries) != 7 {
		t.Fatalf("entries len = %d, want 7: %#v", len(entries.Entries), entries.Entries)
	}
	entry := entries.Entries[0]
	if entry.Key != "session:1" || entry.Type != "string" || entry.ValuePreview != "active user" {
		t.Fatalf("entry = %#v, want session string preview", entry)
	}
	if entry.TTLMillis == nil || *entry.TTLMillis != int64(time.Minute/time.Millisecond) {
		t.Fatalf("entry TTL = %v, want 60000", entry.TTLMillis)
	}
	hllEntry := entries.Entries[1]
	wantHLLPreview := strconv.Itoa(int(hllInfo.Precision)) + " precision, " + strconv.FormatUint(hllInfo.Estimate, 10) + " estimated"
	if hllEntry.Key != "session:card" || hllEntry.Type != "hyperloglog" || hllEntry.SizeBytes != int64(hllInfo.RegisterBytes) || hllEntry.ValuePreview != wantHLLPreview {
		t.Fatalf("hyperloglog entry = %#v, want compact register preview", hllEntry)
	}
	sketchEntry := entries.Entries[2]
	wantSketchPreview := strconv.FormatUint(sketchInfo.Width, 10) + "x" + strconv.Itoa(int(sketchInfo.Depth)) + " counters, " + strconv.FormatUint(sketchInfo.TotalCount, 10) + " total"
	if sketchEntry.Key != "session:freq" || sketchEntry.Type != "count_min_sketch" || sketchEntry.SizeBytes != int64(sketchInfo.CounterBytes) || sketchEntry.ValuePreview != wantSketchPreview {
		t.Fatalf("count-min sketch entry = %#v, want compact counter preview", sketchEntry)
	}
	queueEntry := entries.Entries[3]
	if queueEntry.Key != "session:jobs" || queueEntry.Type != "priority_queue" || queueEntry.SizeBytes != 2 || queueEntry.ValuePreview != "2 priority items" {
		t.Fatalf("priority queue entry = %#v, want priority queue item preview", queueEntry)
	}
	bloomEntry := entries.Entries[4]
	wantBloomPreview := strconv.FormatUint(bloomInfo.BitCount, 10) + " bits, " + strconv.Itoa(int(bloomInfo.HashCount)) + " hashes"
	if bloomEntry.Key != "session:seen" || bloomEntry.Type != "bloom_filter" || bloomEntry.SizeBytes != int64(bloomInfo.BitBytes) || bloomEntry.ValuePreview != wantBloomPreview {
		t.Fatalf("bloom filter entry = %#v, want compact bitset preview", bloomEntry)
	}
	setEntry := entries.Entries[5]
	if setEntry.Key != "session:tags" || setEntry.Type != "set" || setEntry.SizeBytes != 2 || setEntry.ValuePreview != "2 members" {
		t.Fatalf("set entry = %#v, want set member preview", setEntry)
	}
	topKEntry := entries.Entries[6]
	wantTopKPreview := strconv.FormatUint(topKInfo.Tracked, 10) + "/" + strconv.FormatUint(topKInfo.Capacity, 10) + " tracked, " + strconv.FormatUint(topKInfo.Total, 10) + " total"
	if topKEntry.Key != "session:top" || topKEntry.Type != "top_k" || topKEntry.ValuePreview != wantTopKPreview || topKEntry.SizeBytes <= 0 {
		t.Fatalf("top-k entry = %#v, want compact heavy-hitter preview", topKEntry)
	}
}

func TestMonitoringPreviewHandlesInvalidDiskByteIndex(t *testing.T) {
	ht := newTestTrie(t)
	for _, idx := range []int32{-1, 99} {
		size, preview := ht.monitoringPreviewLocked(HatValue{
			Index: idx,
			Flags: DATAVALUE_TYPE_RAW_BYTES | (1 << DATAVALUE_DISK_BIT_SHIFT),
		})
		if size != 0 || preview != "" {
			t.Fatalf("monitoringPreviewLocked(%d) = %d/%q, want empty preview", idx, size, preview)
		}
	}
}

func TestTruncatePreviewPreservesUTF8(t *testing.T) {
	value := strings.Repeat("a", 76) + "\u65e5" + strings.Repeat("b", 20)
	got := truncatePreview(value)
	if !utf8.ValidString(got) {
		t.Fatalf("truncatePreview produced invalid UTF-8: %q", got)
	}
	want := strings.Repeat("a", 76) + "..."
	if got != want {
		t.Fatalf("truncatePreview() = %q, want %q", got, want)
	}
}

func TestMonitoringHandlerServesStaticWebDir(t *testing.T) {
	ht := newTestTrie(t)
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "index.html")
	if err := os.WriteFile(indexPath, []byte("monitoring ui"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	handler := NewMonitoringHandler(ht, MonitoringOptions{WebDir: dir}).Handler()
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("static status = %d, want 200", resp.Code)
	}
	if got := resp.Body.String(); got != "monitoring ui" {
		t.Fatalf("static body = %q, want monitoring ui", got)
	}
}

func TestMonitoringHandlerExecutesCommands(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	setResp := httptest.NewRecorder()
	handler.ServeHTTP(setResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"name","value":"ivi"}`)))
	if setResp.Code != http.StatusOK {
		t.Fatalf("SETSTR status = %d, want 200", setResp.Code)
	}
	var setResult CacheCommandResponse
	if err := json.Unmarshal(setResp.Body.Bytes(), &setResult); err != nil {
		t.Fatalf("SETSTR JSON error = %v", err)
	}
	if !setResult.OK {
		t.Fatalf("SETSTR response = %#v, want ok", setResult)
	}

	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"GET","key":"name"}`)))
	if getResp.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", getResp.Code)
	}
	var getResult CacheCommandResponse
	if err := json.Unmarshal(getResp.Body.Bytes(), &getResult); err != nil {
		t.Fatalf("GET JSON error = %v", err)
	}
	if !getResult.OK || getResult.Value != "ivi" {
		t.Fatalf("GET response = %#v, want ivi", getResult)
	}

	putMapResp := httptest.NewRecorder()
	handler.ServeHTTP(putMapResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"PUTMAP","key":"profile","pairs":{"age":32}}`)))
	if putMapResp.Code != http.StatusOK {
		t.Fatalf("PUTMAP status = %d, want 200", putMapResp.Code)
	}
	var putMapResult CacheCommandResponse
	if err := json.Unmarshal(putMapResp.Body.Bytes(), &putMapResult); err != nil {
		t.Fatalf("PUTMAP JSON error = %v", err)
	}
	if !putMapResult.OK {
		t.Fatalf("PUTMAP response = %#v, want ok", putMapResult)
	}
	if got := ht.PeekMap("profile", "age"); got != json.Number("32") {
		t.Fatalf("profile age = %#v, want json.Number(32)", got)
	}
}

func TestMonitoringHandlerJournalsMutatingCommands(t *testing.T) {
	ht := newTestTrie(t)
	journalPath := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{Journal: journal}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"name","value":"ivi"}`)))
	if resp.Code != http.StatusOK {
		t.Fatalf("SETSTR status = %d, want 200", resp.Code)
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetString("name"); got != "ivi" {
		t.Fatalf("replayed name = %q, want ivi", got)
	}
}

func TestMonitoringHandlerExposesJournalTail(t *testing.T) {
	ht := newTestTrie(t)
	journalPath := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "views", Value: "2"}); !got.OK {
		t.Fatalf("journaled INC response = %#v, want ok", got)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{Journal: journal}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/journal?after_sequence=1", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("journal status = %d, want 200", resp.Code)
	}
	var tail CommandJournalTail
	if err := json.Unmarshal(resp.Body.Bytes(), &tail); err != nil {
		t.Fatalf("journal JSON error = %v", err)
	}
	if tail.LastSequence != 2 || len(tail.Entries) != 1 {
		t.Fatalf("journal tail = %#v, want one entry after sequence 1", tail)
	}
	if entry := tail.Entries[0]; entry.Sequence != 2 || entry.Request.Command != "INC" || entry.Request.Key != "views" {
		t.Fatalf("journal tail entry = %#v, want sequence 2 INC views", entry)
	}

	badSequenceResp := httptest.NewRecorder()
	handler.ServeHTTP(badSequenceResp, httptest.NewRequest(http.MethodGet, "/api/journal?after_sequence=bad", nil))
	if badSequenceResp.Code != http.StatusBadRequest {
		t.Fatalf("bad journal sequence status = %d, want 400", badSequenceResp.Code)
	}

	unconfigured := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	unconfiguredResp := httptest.NewRecorder()
	unconfigured.ServeHTTP(unconfiguredResp, httptest.NewRequest(http.MethodGet, "/api/journal", nil))
	if unconfiguredResp.Code != http.StatusConflict {
		t.Fatalf("unconfigured journal status = %d, want 409", unconfiguredResp.Code)
	}
}

func TestMonitoringHandlerRejectsInvalidCommandRequests(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	for _, method := range []string{http.MethodGet, http.MethodPut} {
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, httptest.NewRequest(method, "/api/commands", nil))
		if resp.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status = %d, want 405", method, resp.Code)
		}
	}

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"GET","key":"x"} trailing`)))
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("invalid JSON status = %d, want 400", resp.Code)
	}
}

func TestMonitoringHandlerSkipsCanceledMutationRequests(t *testing.T) {
	ht := newTestTrie(t)
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	election := NewElectionStore(topology, ElectionOptions{})
	snapshotCalled := false
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		Snapshot: func() error {
			snapshotCalled = true
			return nil
		},
		Topology: topology,
		Election: election,
	}).Handler()

	commandResp := httptest.NewRecorder()
	handler.ServeHTTP(commandResp, canceledMonitoringRequest(http.MethodPost, "/api/commands", `{"command":"SETSTR","key":"name","value":"ivi"}`))
	if commandResp.Code != http.StatusRequestTimeout {
		t.Fatalf("canceled command status = %d, want 408", commandResp.Code)
	}
	if ht.Exists("name") {
		t.Fatal("canceled command stored name")
	}

	snapshotResp := httptest.NewRecorder()
	handler.ServeHTTP(snapshotResp, canceledMonitoringRequest(http.MethodPost, "/api/snapshot", ""))
	if snapshotResp.Code != http.StatusRequestTimeout {
		t.Fatalf("canceled snapshot status = %d, want 408", snapshotResp.Code)
	}
	if snapshotCalled {
		t.Fatal("canceled snapshot request called snapshot callback")
	}

	topologyUpdate := `{"version":1,"self":"node-b","nodes":[{"id":"node-b"}],"shards":[{"id":0,"primary":"node-b"}]}`
	topologyResp := httptest.NewRecorder()
	handler.ServeHTTP(topologyResp, canceledMonitoringRequest(http.MethodPut, "/api/topology", topologyUpdate))
	if topologyResp.Code != http.StatusRequestTimeout {
		t.Fatalf("canceled topology status = %d, want 408", topologyResp.Code)
	}
	if got := topology.Get().Self; got != "node-a" {
		t.Fatalf("topology self = %q after canceled PUT, want node-a", got)
	}

	electionResp := httptest.NewRecorder()
	handler.ServeHTTP(electionResp, canceledMonitoringRequest(http.MethodPost, "/api/election", `{"node":"node-a","online":false}`))
	if electionResp.Code != http.StatusRequestTimeout {
		t.Fatalf("canceled election status = %d, want 408", electionResp.Code)
	}
	if got := election.Status().Leaders[0]; got.Leader != "node-a" {
		t.Fatalf("leader after canceled election update = %#v, want node-a", got)
	}
}

func TestMonitoringHandlerForcesSnapshotWhenConfigured(t *testing.T) {
	ht := newTestTrie(t)
	called := false
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		Snapshot: func() error {
			called = true
			return nil
		},
	}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/snapshot", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d, want 200", resp.Code)
	}
	if !called {
		t.Fatal("snapshot callback was not called")
	}
	var result CacheCommandResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("snapshot JSON error = %v", err)
	}
	if !result.OK {
		t.Fatalf("snapshot response = %#v, want ok", result)
	}
}

func TestMonitoringHandlerRejectsForcedSnapshotWhenUnconfigured(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/snapshot", nil))
	if resp.Code != http.StatusConflict {
		t.Fatalf("snapshot status = %d, want 409", resp.Code)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/snapshot", nil))
	if resp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("snapshot GET status = %d, want 405", resp.Code)
	}
}

func TestMonitoringHandlerManagesTopology(t *testing.T) {
	ht := newTestTrie(t)
	store, err := NewTopologyStore(SingleNodeTopology("node-a", "127.0.0.1:8080"))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{Topology: store}).Handler()

	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, httptest.NewRequest(http.MethodGet, "/api/topology", nil))
	if getResp.Code != http.StatusOK {
		t.Fatalf("topology GET status = %d, want 200", getResp.Code)
	}
	var got ClusterTopology
	if err := json.Unmarshal(getResp.Body.Bytes(), &got); err != nil {
		t.Fatalf("topology GET JSON error = %v", err)
	}
	if got.Self != "node-a" || len(got.Shards) != 1 {
		t.Fatalf("topology GET = %#v, want node-a single shard", got)
	}

	update := `{"version":1,"self":"node-b","nodes":[{"id":"node-b","address":"127.0.0.1:8081"}],"shards":[{"id":0,"primary":"node-b"}]}`
	putResp := httptest.NewRecorder()
	handler.ServeHTTP(putResp, httptest.NewRequest(http.MethodPut, "/api/topology", bytes.NewBufferString(update)))
	if putResp.Code != http.StatusOK {
		t.Fatalf("topology PUT status = %d body %q, want 200", putResp.Code, putResp.Body.String())
	}
	if store.Get().Self != "node-b" {
		t.Fatalf("stored topology = %#v, want node-b", store.Get())
	}

	routeResp := httptest.NewRecorder()
	handler.ServeHTTP(routeResp, httptest.NewRequest(http.MethodGet, "/api/topology?key=session:1", nil))
	if routeResp.Code != http.StatusOK {
		t.Fatalf("topology route status = %d, want 200", routeResp.Code)
	}
	var route TopologyRoute
	if err := json.Unmarshal(routeResp.Body.Bytes(), &route); err != nil {
		t.Fatalf("route JSON error = %v", err)
	}
	if route.Key != "session:1" || route.Shard.Primary != "node-b" {
		t.Fatalf("route = %#v, want node-b primary", route)
	}

	badResp := httptest.NewRecorder()
	handler.ServeHTTP(badResp, httptest.NewRequest(http.MethodPut, "/api/topology", bytes.NewBufferString(`{"version":1}`)))
	if badResp.Code != http.StatusBadRequest {
		t.Fatalf("bad topology status = %d, want 400", badResp.Code)
	}
}

func TestMonitoringHandlerManagesElection(t *testing.T) {
	ht := newTestTrie(t)
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Nodes: []TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	election := NewElectionStore(topology, ElectionOptions{})
	handler := NewMonitoringHandler(ht, MonitoringOptions{Topology: topology, Election: election}).Handler()

	statusResp := httptest.NewRecorder()
	handler.ServeHTTP(statusResp, httptest.NewRequest(http.MethodGet, "/api/election", nil))
	if statusResp.Code != http.StatusOK {
		t.Fatalf("election GET status = %d, want 200", statusResp.Code)
	}
	var status ElectionStatus
	if err := json.Unmarshal(statusResp.Body.Bytes(), &status); err != nil {
		t.Fatalf("election status JSON error = %v", err)
	}
	if len(status.Leaders) != 1 || status.Leaders[0].Leader != "node-a" {
		t.Fatalf("election status = %#v, want node-a leader", status)
	}

	offlineResp := httptest.NewRecorder()
	handler.ServeHTTP(offlineResp, httptest.NewRequest(http.MethodPost, "/api/election", bytes.NewBufferString(`{"node":"node-a","online":false}`)))
	if offlineResp.Code != http.StatusOK {
		t.Fatalf("election offline status = %d body %q, want 200", offlineResp.Code, offlineResp.Body.String())
	}
	if got := election.Status().Leaders[0]; got.Leader != "node-b" {
		t.Fatalf("leader after offline = %#v, want node-b", got)
	}

	routeResp := httptest.NewRecorder()
	handler.ServeHTTP(routeResp, httptest.NewRequest(http.MethodGet, "/api/election?key=session:1", nil))
	if routeResp.Code != http.StatusOK {
		t.Fatalf("election route status = %d, want 200", routeResp.Code)
	}
	var route ElectionKeyRoute
	if err := json.Unmarshal(routeResp.Body.Bytes(), &route); err != nil {
		t.Fatalf("election route JSON error = %v", err)
	}
	if route.Key != "session:1" || route.Leader.Leader != "node-b" {
		t.Fatalf("election route = %#v, want node-b leader", route)
	}

	badResp := httptest.NewRecorder()
	handler.ServeHTTP(badResp, httptest.NewRequest(http.MethodPost, "/api/election", bytes.NewBufferString(`{"node":"missing"}`)))
	if badResp.Code != http.StatusBadRequest {
		t.Fatalf("bad election status = %d, want 400", badResp.Code)
	}

	methodResp := httptest.NewRecorder()
	handler.ServeHTTP(methodResp, httptest.NewRequest(http.MethodDelete, "/api/election", nil))
	if methodResp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("election DELETE status = %d, want 405", methodResp.Code)
	}
}

func TestMonitoringHandlerReplicatesCommands(t *testing.T) {
	ht := newTestTrie(t)
	var gotRequest CacheCommandRequest
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
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
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:   "node-a",
		Topology:   topology,
		Election:   election,
		Replicator: replicator,
	}).Handler()

	commandResp := httptest.NewRecorder()
	handler.ServeHTTP(commandResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"session:1","value":"value"}`)))
	if commandResp.Code != http.StatusOK {
		t.Fatalf("command status = %d, want 200", commandResp.Code)
	}
	if gotRequest.Command != "INTERNALSET" || gotRequest.Key != "session:1" || gotRequest.Value == "" {
		t.Fatalf("replicated request = %#v, want INTERNALSET snapshot", gotRequest)
	}

	replicationResp := httptest.NewRecorder()
	handler.ServeHTTP(replicationResp, httptest.NewRequest(http.MethodGet, "/api/replication", nil))
	if replicationResp.Code != http.StatusOK {
		t.Fatalf("replication status = %d, want 200", replicationResp.Code)
	}
	var result ReplicationResult
	if err := json.Unmarshal(replicationResp.Body.Bytes(), &result); err != nil {
		t.Fatalf("replication JSON error = %v", err)
	}
	if len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("replication result = %#v, want one ok target", result)
	}
}

func TestMonitoringHandlerSyncsReplication(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("session:1", "value")
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		var request CacheCommandRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		requests <- request
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
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:   "node-a",
		Topology:   topology,
		Election:   election,
		Replicator: replicator,
	}).Handler()

	replicationResp := httptest.NewRecorder()
	handler.ServeHTTP(replicationResp, httptest.NewRequest(http.MethodPost, "/api/replication", bytes.NewBufferString(`{"prefix":"session:"}`)))
	if replicationResp.Code != http.StatusOK {
		t.Fatalf("replication sync status = %d, want 200", replicationResp.Code)
	}
	var result ReplicationResult
	if err := json.Unmarshal(replicationResp.Body.Bytes(), &result); err != nil {
		t.Fatalf("replication sync JSON error = %v", err)
	}
	if result.Skipped || result.Command != "SYNC" || result.Key != "session:" || result.Entries != 1 || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("replication sync result = %#v, want one ok target", result)
	}
	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "session:1" || request.Value == "" {
			t.Fatalf("replication sync request = %#v, want INTERNALSET snapshot", request)
		}
	default:
		t.Fatal("replication sync did not reach remote target")
	}

	invalidResp := httptest.NewRecorder()
	handler.ServeHTTP(invalidResp, httptest.NewRequest(http.MethodPost, "/api/replication", bytes.NewBufferString(`{"unknown":true}`)))
	if invalidResp.Code != http.StatusBadRequest {
		t.Fatalf("invalid replication sync status = %d, want 400", invalidResp.Code)
	}
}

func canceledMonitoringRequest(method string, target string, body string) *http.Request {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return httptest.NewRequest(method, target, bytes.NewBufferString(body)).WithContext(ctx)
}
