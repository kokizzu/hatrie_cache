package hatriecache

import (
	"context"
	"net"
	"path/filepath"
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
