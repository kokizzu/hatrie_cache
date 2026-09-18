package hatCache

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

type tu37RecordingThrottle struct {
	entries []int
}

func (throttle *tu37RecordingThrottle) Wait(_ context.Context, entries int) error {
	throttle.entries = append(throttle.entries, entries)
	return nil
}

func TestTU37GRPCReplicationStreamApplierUsesThrottle(t *testing.T) {
	topology := replicationTestTopology(t, "http://node-b")
	target := newTestTrie(t)
	recorder := &tu37RecordingThrottle{}
	server := NewCacheGRPCServer(target, CacheGRPCOptions{
		NodeName:                 "node-b",
		Topology:                 topology,
		ReplicationSafety:        NewReplicationSafetyStore(),
		ReplicationApplyThrottle: recorder,
	})
	source := newTestTrie(t)
	source.UpsertString("tu37:key", "value")
	payload, ok := replicationCommandPayload(source, "tu37:key", replicationPayloadSet)
	if !ok {
		t.Fatal("replicationCommandPayload() ok = false")
	}
	ack := server.applyReplicationStreamBatch(context.Background(), &hatriecachev1.ReplicationStreamBatch{
		Source:              "node-a",
		Sequence:            1,
		TopologyFingerprint: topology.Fingerprint(),
		Keys:                []string{"tu37:key"},
		BinaryValues:        [][]byte{payload.BinaryValue},
	})
	if !ack.GetOk() || ack.GetEntries() != 1 {
		t.Fatalf("acknowledgement = %#v, want one applied entry", ack)
	}
	if len(recorder.entries) != 1 || recorder.entries[0] != 1 {
		t.Fatalf("throttle entries = %#v, want [1]", recorder.entries)
	}
}

func TestTU37JournalPullApplierUsesThrottle(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := json.NewEncoder(w).Encode(CommandJournalTail{
			LastSequence: 2,
			Entries: []CommandJournalRecord{
				{Sequence: 1, Request: CacheCommandRequest{Command: "SETSTR", Key: "tu37:one", Value: "one"}},
				{Sequence: 2, Request: CacheCommandRequest{Command: "SETSTR", Key: "tu37:two", Value: "two"}},
			},
		}); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}))
	defer source.Close()

	target := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	recorder := &tu37RecordingThrottle{}
	result, err := PullCommandJournal(context.Background(), target, journal, CommandJournalPullOptions{
		Source:                   source.URL,
		ReplicationApplyThrottle: recorder,
	})
	if err != nil {
		t.Fatalf("PullCommandJournal() error = %v", err)
	}
	if result.Applied != 2 || target.GetString("tu37:one") != "one" || target.GetString("tu37:two") != "two" {
		t.Fatalf("pull result = %#v, values = %q/%q", result, target.GetString("tu37:one"), target.GetString("tu37:two"))
	}
	if len(recorder.entries) != 1 || recorder.entries[0] != 2 {
		t.Fatalf("throttle entries = %#v, want [2]", recorder.entries)
	}
}
