package hatriecache

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

const testGRPCBufferSize = 1024 * 1024

func TestCacheGRPCServerHealthStatsEntriesAndCommands(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(2000, 0)
	ht.now = func() time.Time { return now }
	ht.UpsertString("session:1", "active user")
	if !ht.Expire("session:1", time.Minute) {
		t.Fatal("Expire(session:1) = false, want true")
	}

	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		NodeName: "test-node",
		StartAt:  time.Now().Add(-time.Hour),
	})
	defer stop()

	health, err := client.Health(context.Background(), &hatriecachev1.HealthRequest{})
	if err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if health.GetStatus() != "online" || health.GetNode() != "test-node" {
		t.Fatalf("Health() = %#v, want online test-node", health)
	}

	setResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "name",
		Value:   "ivi",
	})
	if err != nil {
		t.Fatalf("Command(SETSTR) error = %v", err)
	}
	if !setResp.GetOk() {
		t.Fatalf("SETSTR response = %#v, want ok", setResp)
	}

	ttlSeconds := int64(30)
	setIntResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command:    "SETINT",
		Key:        "views",
		Value:      "41",
		TtlSeconds: &ttlSeconds,
	})
	if err != nil {
		t.Fatalf("Command(SETINT) error = %v", err)
	}
	if !setIntResp.GetOk() {
		t.Fatalf("SETINT response = %#v, want ok", setIntResp)
	}
	incResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INC",
		Key:     "views",
	})
	if err != nil {
		t.Fatalf("Command(INC) error = %v", err)
	}
	if !incResp.GetOk() || incResp.GetValue() != "42" {
		t.Fatalf("INC response = %#v, want 42", incResp)
	}

	putMapResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs:   map[string]string{"city": "Singapore"},
	})
	if err != nil {
		t.Fatalf("Command(PUTMAP) error = %v", err)
	}
	if !putMapResp.GetOk() {
		t.Fatalf("PUTMAP response = %#v, want ok", putMapResp)
	}
	peekMapResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "PEEKMAP",
		Key:     "profile",
		Subkey:  "city",
	})
	if err != nil {
		t.Fatalf("Command(PEEKMAP) error = %v", err)
	}
	if !peekMapResp.GetOk() || peekMapResp.GetValue() != "Singapore" {
		t.Fatalf("PEEKMAP response = %#v, want Singapore", peekMapResp)
	}

	pushResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "PUSHSLICE",
		Key:     "jobs",
		Values:  []string{"build", "verify"},
	})
	if err != nil {
		t.Fatalf("Command(PUSHSLICE) error = %v", err)
	}
	if !pushResp.GetOk() {
		t.Fatalf("PUSHSLICE response = %#v, want ok", pushResp)
	}
	tailResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "TAILSLICE",
		Key:     "jobs",
	})
	if err != nil {
		t.Fatalf("Command(TAILSLICE) error = %v", err)
	}
	if !tailResp.GetOk() || tailResp.GetValue() != "verify" {
		t.Fatalf("TAILSLICE response = %#v, want verify", tailResp)
	}

	priority := int64(1)
	pushPQResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command:  "PUSHPQ",
		Key:      "priority:jobs",
		Value:    "urgent",
		Priority: &priority,
	})
	if err != nil {
		t.Fatalf("Command(PUSHPQ) error = %v", err)
	}
	if !pushPQResp.GetOk() {
		t.Fatalf("PUSHPQ response = %#v, want ok", pushPQResp)
	}
	popPQResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "POPPQ",
		Key:     "priority:jobs",
	})
	if err != nil {
		t.Fatalf("Command(POPPQ) error = %v", err)
	}
	if !popPQResp.GetOk() || popPQResp.GetValue() != `{"priority":1,"value":"urgent"}` {
		t.Fatalf("POPPQ response = %#v, want urgent priority item", popPQResp)
	}

	createBloomResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATEBF",
		Key:     "seen",
		Value:   "1000",
		Subkey:  "0.001",
	})
	if err != nil {
		t.Fatalf("Command(CREATEBF) error = %v", err)
	}
	if !createBloomResp.GetOk() {
		t.Fatalf("CREATEBF response = %#v, want ok", createBloomResp)
	}
	addBloomResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDBF",
		Key:     "seen",
		Values:  []string{"alpha", "beta"},
	})
	if err != nil {
		t.Fatalf("Command(ADDBF) error = %v", err)
	}
	if !addBloomResp.GetOk() || addBloomResp.GetValue() != "2" {
		t.Fatalf("ADDBF response = %#v, want added 2", addBloomResp)
	}
	hasBloomResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "HASBF",
		Key:     "seen",
		Value:   "alpha",
	})
	if err != nil {
		t.Fatalf("Command(HASBF) error = %v", err)
	}
	if !hasBloomResp.GetOk() || hasBloomResp.GetValue() != "1" {
		t.Fatalf("HASBF response = %#v, want hit", hasBloomResp)
	}
	infoBloomResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOBF",
		Key:     "seen",
	})
	if err != nil {
		t.Fatalf("Command(INFOBF) error = %v", err)
	}
	if !infoBloomResp.GetOk() || infoBloomResp.GetValue() == "" {
		t.Fatalf("INFOBF response = %#v, want JSON info", infoBloomResp)
	}

	createSketchResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATECMS",
		Key:     "freq",
		Value:   "128",
		Subkey:  "4",
	})
	if err != nil {
		t.Fatalf("Command(CREATECMS) error = %v", err)
	}
	if !createSketchResp.GetOk() {
		t.Fatalf("CREATECMS response = %#v, want ok", createSketchResp)
	}
	incrSketchResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INCRCMS",
		Key:     "freq",
		Value:   "alpha",
		Subkey:  "3",
	})
	if err != nil {
		t.Fatalf("Command(INCRCMS) error = %v", err)
	}
	if !incrSketchResp.GetOk() || incrSketchResp.GetValue() != "3" {
		t.Fatalf("INCRCMS response = %#v, want estimate 3", incrSketchResp)
	}
	estimateSketchResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ESTCMS",
		Key:     "freq",
		Value:   "alpha",
	})
	if err != nil {
		t.Fatalf("Command(ESTCMS) error = %v", err)
	}
	if !estimateSketchResp.GetOk() || estimateSketchResp.GetValue() != "3" {
		t.Fatalf("ESTCMS response = %#v, want estimate 3", estimateSketchResp)
	}
	infoSketchResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOCMS",
		Key:     "freq",
	})
	if err != nil {
		t.Fatalf("Command(INFOCMS) error = %v", err)
	}
	if !infoSketchResp.GetOk() || infoSketchResp.GetValue() == "" {
		t.Fatalf("INFOCMS response = %#v, want JSON info", infoSketchResp)
	}

	createHLLResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATEHLL",
		Key:     "card",
		Value:   "10",
	})
	if err != nil {
		t.Fatalf("Command(CREATEHLL) error = %v", err)
	}
	if !createHLLResp.GetOk() {
		t.Fatalf("CREATEHLL response = %#v, want ok", createHLLResp)
	}
	addHLLResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDHLL",
		Key:     "card",
		Values:  []string{"alpha", "beta"},
	})
	if err != nil {
		t.Fatalf("Command(ADDHLL) error = %v", err)
	}
	if !addHLLResp.GetOk() || addHLLResp.GetValue() == "0" {
		t.Fatalf("ADDHLL response = %#v, want non-zero estimate", addHLLResp)
	}
	countHLLResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "COUNTHLL",
		Key:     "card",
	})
	if err != nil {
		t.Fatalf("Command(COUNTHLL) error = %v", err)
	}
	if !countHLLResp.GetOk() || countHLLResp.GetValue() != addHLLResp.GetValue() {
		t.Fatalf("COUNTHLL response = %#v, want estimate %q", countHLLResp, addHLLResp.GetValue())
	}
	infoHLLResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOHLL",
		Key:     "card",
	})
	if err != nil {
		t.Fatalf("Command(INFOHLL) error = %v", err)
	}
	if !infoHLLResp.GetOk() || infoHLLResp.GetValue() == "" {
		t.Fatalf("INFOHLL response = %#v, want JSON info", infoHLLResp)
	}

	createTopKResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "CREATETOPK",
		Key:     "top",
		Value:   "3",
	})
	if err != nil {
		t.Fatalf("Command(CREATETOPK) error = %v", err)
	}
	if !createTopKResp.GetOk() {
		t.Fatalf("CREATETOPK response = %#v, want ok", createTopKResp)
	}
	addTopKResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "ADDTOPK",
		Key:     "top",
		Value:   "alpha",
		Subkey:  "5",
	})
	if err != nil {
		t.Fatalf("Command(ADDTOPK) error = %v", err)
	}
	if !addTopKResp.GetOk() || addTopKResp.GetValue() == "" {
		t.Fatalf("ADDTOPK response = %#v, want JSON estimate", addTopKResp)
	}
	getTopKResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "GETTOPK",
		Key:     "top",
	})
	if err != nil {
		t.Fatalf("Command(GETTOPK) error = %v", err)
	}
	if !getTopKResp.GetOk() || getTopKResp.GetValue() == "" {
		t.Fatalf("GETTOPK response = %#v, want JSON items", getTopKResp)
	}
	infoTopKResp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INFOTOPK",
		Key:     "top",
	})
	if err != nil {
		t.Fatalf("Command(INFOTOPK) error = %v", err)
	}
	if !infoTopKResp.GetOk() || infoTopKResp.GetValue() == "" {
		t.Fatalf("INFOTOPK response = %#v, want JSON info", infoTopKResp)
	}

	stats, err := client.Stats(context.Background(), &hatriecachev1.StatsRequest{})
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if stats.GetWrites() == 0 || stats.GetReads() == 0 {
		t.Fatalf("Stats() = %#v, want reads and writes", stats)
	}
	if stats.GetLastWriteUnixNano() == 0 {
		t.Fatalf("LastWriteUnixNano = 0, want write timestamp")
	}

	entries, err := client.Entries(context.Background(), &hatriecachev1.EntriesRequest{Prefix: "session:"})
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries.GetEntries()) != 1 {
		t.Fatalf("entries len = %d, want 1: %#v", len(entries.GetEntries()), entries.GetEntries())
	}
	entry := entries.GetEntries()[0]
	if entry.GetKey() != "session:1" || entry.GetType() != "string" || entry.GetValuePreview() != "active user" {
		t.Fatalf("entry = %#v, want session string", entry)
	}
	if entry.TtlMillis == nil || entry.GetTtlMillis() != int64(time.Minute/time.Millisecond) {
		t.Fatalf("entry ttl = %v, want 60000", entry.TtlMillis)
	}
}

func TestCacheGRPCServerSnapshotAndJournal(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	snapshotCalled := false
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		Journal: journal,
		Snapshot: func() error {
			snapshotCalled = true
			return nil
		},
	})
	defer stop()

	resp, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "name",
		Value:   "ivi",
	})
	if err != nil {
		t.Fatalf("Command(SETSTR) error = %v", err)
	}
	if !resp.GetOk() {
		t.Fatalf("SETSTR response = %#v, want ok", resp)
	}
	if journal.Sequence() != 1 {
		t.Fatalf("journal sequence = %d, want 1", journal.Sequence())
	}

	snapshotResp, err := client.Snapshot(context.Background(), &hatriecachev1.SnapshotRequest{})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if !snapshotResp.GetOk() || !snapshotCalled {
		t.Fatalf("Snapshot response/called = %#v/%v, want ok true", snapshotResp, snapshotCalled)
	}
}

func TestCacheGRPCServerEnforcesLeaderWrites(t *testing.T) {
	topology := replicationTestTopology(t, "127.0.0.1:1")
	election := NewElectionStore(topology, ElectionOptions{})
	if err := election.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(node-a) error = %v", err)
	}

	followerTrie := newTestTrie(t)
	followerClient, followerStop := newTestGRPCClient(t, followerTrie, CacheGRPCOptions{
		NodeName:            "node-a",
		Election:            election,
		EnforceLeaderWrites: true,
	})
	defer followerStop()

	rejected, err := followerClient.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "session:1",
		Value:   "value",
	})
	if err != nil {
		t.Fatalf("follower Command(SETSTR) error = %v", err)
	}
	if rejected.GetOk() || !strings.Contains(rejected.GetMessage(), "leader is node-b") {
		t.Fatalf("follower SETSTR response = %#v, want leader rejection", rejected)
	}
	if got := followerTrie.GetString("session:1"); got != "" {
		t.Fatalf("follower wrote value %q, want no local write", got)
	}

	internal, err := followerClient.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "INTERNALSET",
		Key:     "session:1",
		Value:   `{"type":"string","string":"replicated"}`,
	})
	if err != nil {
		t.Fatalf("follower Command(INTERNALSET) error = %v", err)
	}
	if !internal.GetOk() {
		t.Fatalf("follower INTERNALSET response = %#v, want ok", internal)
	}
	if got := followerTrie.GetString("session:1"); got != "replicated" {
		t.Fatalf("internal replicated value = %q, want replicated", got)
	}

	leaderTrie := newTestTrie(t)
	leaderClient, leaderStop := newTestGRPCClient(t, leaderTrie, CacheGRPCOptions{
		NodeName:            "node-b",
		Election:            election,
		EnforceLeaderWrites: true,
	})
	defer leaderStop()
	stored, err := leaderClient.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "session:1",
		Value:   "leader-value",
	})
	if err != nil {
		t.Fatalf("leader Command(SETSTR) error = %v", err)
	}
	if !stored.GetOk() {
		t.Fatalf("leader SETSTR response = %#v, want ok", stored)
	}
	if got := leaderTrie.GetString("session:1"); got != "leader-value" {
		t.Fatalf("leader wrote value %q, want leader-value", got)
	}
}

func TestCacheGRPCServerReplicatesCommands(t *testing.T) {
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
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	ht := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		NodeName:   "node-a",
		Election:   election,
		Replicator: replicator,
	})
	defer stop()

	response, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "SETSTR",
		Key:     "session:1",
		Value:   "value",
	})
	if err != nil {
		t.Fatalf("Command(SETSTR) error = %v", err)
	}
	if !response.GetOk() {
		t.Fatalf("SETSTR response = %#v, want ok", response)
	}
	select {
	case request := <-requests:
		if request.Command != "INTERNALSET" || request.Key != "session:1" || request.Value == "" {
			t.Fatalf("replicated request = %#v, want INTERNALSET snapshot", request)
		}
	default:
		t.Fatal("gRPC write did not reach replication target")
	}
}

func TestCacheGRPCServerSnapshotRequiresCallback(t *testing.T) {
	ht := newTestTrie(t)
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{})
	defer stop()

	resp, err := client.Snapshot(context.Background(), &hatriecachev1.SnapshotRequest{})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if resp.GetOk() {
		t.Fatalf("Snapshot response = %#v, want not ok", resp)
	}
}

func TestCacheGRPCServerHonorsCanceledContexts(t *testing.T) {
	ht := newTestTrie(t)
	server := NewCacheGRPCServer(ht, CacheGRPCOptions{
		Snapshot: func() error {
			t.Fatal("snapshot callback should not run for canceled context")
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := server.Health(ctx, &hatriecachev1.HealthRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Health(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := server.Stats(ctx, &hatriecachev1.StatsRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stats(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := server.Entries(ctx, &hatriecachev1.EntriesRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Entries(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := server.Command(ctx, &hatriecachev1.CommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Command(canceled) error = %v, want context.Canceled", err)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("Command(canceled) mutated trie: name=%q", got)
	}
	if _, err := server.Snapshot(ctx, &hatriecachev1.SnapshotRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Snapshot(canceled) error = %v, want context.Canceled", err)
	}
}

func newTestGRPCClient(t *testing.T, ht *HatTrie, options CacheGRPCOptions) (hatriecachev1.CacheServiceClient, func()) {
	t.Helper()

	listener := bufconn.Listen(testGRPCBufferSize)
	server := grpc.NewServer()
	RegisterCacheGRPCServer(server, NewCacheGRPCServer(ht, options))
	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("grpc Serve() error = %v", err)
		}
	}()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}

	stop := func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		server.Stop()
		if err := listener.Close(); err != nil {
			t.Fatalf("listener Close() error = %v", err)
		}
	}
	return hatriecachev1.NewCacheServiceClient(conn), stop
}
