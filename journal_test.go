package hatriecache

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestCommandJournalReplaysMutatingCommands(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "views"}); !got.OK {
		t.Fatalf("journaled INC response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "GET", Key: "name"}); !got.OK || got.Value != "ivi" {
		t.Fatalf("journaled GET response = %#v, want ivi", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("journal entries = %d, want 2", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetString("name"); got != "ivi" {
		t.Fatalf("replayed name = %q, want ivi", got)
	}
	if got := replayed.GetCounter("views"); got != 1 {
		t.Fatalf("replayed views = %d, want 1", got)
	}
}

func TestCommandJournalTailReturnsEntriesAfterSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "GET", Key: "name"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "views", Value: "2"})

	tail, err := journal.Tail(1, 0)
	if err != nil {
		t.Fatalf("Tail() error = %v", err)
	}
	if tail.LastSequence != 2 || tail.CompactedThrough != 0 || len(tail.Entries) != 1 {
		t.Fatalf("tail = %#v, want one entry after sequence 1 with last sequence 2", tail)
	}
	entry := tail.Entries[0]
	if entry.Sequence != 2 || entry.Request.Command != "INC" || entry.Request.Key != "views" {
		t.Fatalf("tail entry = %#v, want sequence 2 INC views", entry)
	}
}

func TestCommandJournalTailLimitReportsMoreEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "one", Value: "1"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "two", Value: "2"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "three", Value: "3"})

	tail, err := journal.Tail(0, 2)
	if err != nil {
		t.Fatalf("Tail(limit) error = %v", err)
	}
	if tail.LastSequence != 3 || tail.Limit != 2 || !tail.HasMore || len(tail.Entries) != 2 {
		t.Fatalf("limited tail = %#v, want first two entries and has_more", tail)
	}
	if tail.Entries[0].Sequence != 1 || tail.Entries[1].Sequence != 2 {
		t.Fatalf("limited tail entries = %#v, want sequences 1 and 2", tail.Entries)
	}

	next, err := journal.Tail(tail.Entries[1].Sequence, 2)
	if err != nil {
		t.Fatalf("Tail(next) error = %v", err)
	}
	if next.HasMore || len(next.Entries) != 1 || next.Entries[0].Sequence != 3 {
		t.Fatalf("next tail = %#v, want final sequence only", next)
	}
}

func TestCommandJournalReplaysSetAndInternalReplicationCommands(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ADDSET", Key: "tags", Values: Slice{"go", "cache"}})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "REMSET", Key: "tags", Value: "go"})
	payload := `{"key":"replicated","type":"string","string":"value"}`
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INTERNALSET", Key: "replicated", Value: payload}); !got.OK {
		t.Fatalf("journaled INTERNALSET response = %#v, want ok", got)
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetSet("tags"); !reflect.DeepEqual(got, Set{"cache"}) {
		t.Fatalf("replayed tags = %#v, want cache", got)
	}
	if got := replayed.GetString("replicated"); got != "value" {
		t.Fatalf("replayed replicated = %q, want value", got)
	}
}

func TestCommandJournalReplaysPriorityQueueMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	priority := int64(1)
	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "PUSHPQ", Key: "jobs", Value: "urgent", Priority: &priority}); !got.OK {
		t.Fatalf("journaled PUSHPQ response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "POPPQ", Key: "jobs"}); !got.OK {
		t.Fatalf("journaled POPPQ response = %#v, want ok", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("journal entries = %d, want 2", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetPriorityQueue("jobs"); len(got) != 0 {
		t.Fatalf("replayed queue = %#v, want empty queue after push/pop", got)
	}
}

func TestCommandJournalReplaysBloomFilterMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "CREATEBF", Key: "seen", Value: "1000", Subkey: "0.001"}); !got.OK {
		t.Fatalf("journaled CREATEBF response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ADDBF", Key: "seen", Values: Slice{"alpha", "beta"}}); !got.OK {
		t.Fatalf("journaled ADDBF response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "HASBF", Key: "seen", Value: "alpha"}); !got.OK || got.Value != "1" {
		t.Fatalf("journaled HASBF response = %#v, want local read hit", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("journal entries = %d, want CREATEBF and ADDBF only", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if !replayed.HasBloomFilter("seen", "alpha") || !replayed.HasBloomFilter("seen", "beta") {
		t.Fatal("replayed Bloom filter does not contain journaled values")
	}
	if info, ok := replayed.BloomFilterInfo("seen"); !ok || info.Insertions != 2 {
		t.Fatalf("replayed BloomFilterInfo = %#v/%v, want 2 insertions", info, ok)
	}
}

func TestCommandJournalReplaysCuckooFilterMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "CREATECF", Key: "seen", Value: "128", Subkey: "0.001"}); !got.OK {
		t.Fatalf("journaled CREATECF response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ADDCF", Key: "seen", Values: Slice{"alpha", "beta"}}); !got.OK {
		t.Fatalf("journaled ADDCF response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "DELCF", Key: "seen", Value: "alpha"}); !got.OK {
		t.Fatalf("journaled DELCF response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "HASCF", Key: "seen", Value: "beta"}); !got.OK || got.Value != "1" {
		t.Fatalf("journaled HASCF response = %#v, want local read hit", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("journal entries = %d, want CREATECF, ADDCF, and DELCF only", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if replayed.HasCuckooFilter("seen", "alpha") {
		t.Fatal("replayed Cuckoo filter contains deleted value alpha")
	}
	if !replayed.HasCuckooFilter("seen", "beta") {
		t.Fatal("replayed Cuckoo filter does not contain journaled value beta")
	}
}

func TestCommandJournalReplaysRoaringBitmapMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "CREATERB", Key: "ids"}); !got.OK {
		t.Fatalf("journaled CREATERB response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ADDRB", Key: "ids", Values: Slice{json.Number("1"), json.Number("65543")}}); !got.OK {
		t.Fatalf("journaled ADDRB response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "REMRB", Key: "ids", Value: "1"}); !got.OK {
		t.Fatalf("journaled REMRB response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "HASRB", Key: "ids", Value: "65543"}); !got.OK || got.Value != "1" {
		t.Fatalf("journaled HASRB response = %#v, want local read hit", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("journal entries = %d, want CREATERB, ADDRB, and REMRB only", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if replayed.HasRoaringBitmap("ids", 1) {
		t.Fatal("replayed Roaring bitmap contains removed value 1")
	}
	if !replayed.HasRoaringBitmap("ids", 65543) {
		t.Fatal("replayed Roaring bitmap does not contain journaled value 65543")
	}
}

func TestCommandJournalReplaysCountMinSketchMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "CREATECMS", Key: "freq", Value: "128", Subkey: "4"}); !got.OK {
		t.Fatalf("journaled CREATECMS response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INCRCMS", Key: "freq", Value: "alpha", Subkey: "2"}); !got.OK {
		t.Fatalf("journaled INCRCMS response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ESTCMS", Key: "freq", Value: "alpha"}); !got.OK || got.Value != "2" {
		t.Fatalf("journaled ESTCMS response = %#v, want local estimate", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("journal entries = %d, want CREATECMS and INCRCMS only", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got, ok := replayed.EstimateCountMinSketch("freq", "alpha"); !ok || got != 2 {
		t.Fatalf("replayed Count-Min Sketch estimate = %d/%v, want 2", got, ok)
	}
	if info, ok := replayed.CountMinSketchInfo("freq"); !ok || info.TotalCount != 2 {
		t.Fatalf("replayed CountMinSketchInfo = %#v/%v, want total 2", info, ok)
	}
}

func TestCommandJournalReplaysHyperLogLogMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "CREATEHLL", Key: "card", Value: "10"}); !got.OK {
		t.Fatalf("journaled CREATEHLL response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ADDHLL", Key: "card", Values: Slice{"alpha", "beta"}}); !got.OK {
		t.Fatalf("journaled ADDHLL response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "COUNTHLL", Key: "card"}); !got.OK || got.Value == "0" {
		t.Fatalf("journaled COUNTHLL response = %#v, want local estimate", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("journal entries = %d, want CREATEHLL and ADDHLL only", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got, ok := replayed.CountHyperLogLog("card"); !ok || got < 2 {
		t.Fatalf("replayed HyperLogLog estimate = %d/%v, want at least 2", got, ok)
	}
	if info, ok := replayed.HyperLogLogInfo("card"); !ok || info.Observations != 2 {
		t.Fatalf("replayed HyperLogLogInfo = %#v/%v, want 2 observations", info, ok)
	}
}

func TestCommandJournalReplaysTopKMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "CREATETOPK", Key: "top", Value: "3"}); !got.OK {
		t.Fatalf("journaled CREATETOPK response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ADDTOPK", Key: "top", Value: "alpha", Subkey: "5"}); !got.OK {
		t.Fatalf("journaled ADDTOPK response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "GETTOPK", Key: "top"}); !got.OK || got.Value == "" {
		t.Fatalf("journaled GETTOPK response = %#v, want local items", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("journal entries = %d, want CREATETOPK and ADDTOPK only", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.EstimateTopK("top", "alpha"); !got.Tracked || got.Count != 5 {
		t.Fatalf("replayed Top-K estimate = %#v, want alpha count 5", got)
	}
	if info, ok := replayed.TopKInfo("top"); !ok || info.Total != 5 || info.Tracked != 1 {
		t.Fatalf("replayed TopKInfo = %#v/%v, want total 5", info, ok)
	}
}

func TestCommandJournalReplaysRelativeTTLsAsAbsoluteExpirations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	now := time.Unix(2000, 0)
	ttlSeconds := int64(10)
	ht := newTestTrie(t)
	ht.now = func() time.Time { return now }
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "temp", Value: "value", TTLSeconds: &ttlSeconds}); !got.OK {
		t.Fatalf("journaled SETSTR ttl response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "refresh", Value: "value"}); !got.OK {
		t.Fatalf("journaled SETSTR refresh response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "EXPIRE", Key: "refresh", TTLSeconds: &ttlSeconds}); !got.OK {
		t.Fatalf("journaled EXPIRE response = %#v, want ok", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if got, want := len(entries), 3; got != want {
		t.Fatalf("journal entries = %d, want %d", got, want)
	}
	for _, idx := range []int{0, 2} {
		if entries[idx].Request.TTLSeconds != nil {
			t.Fatalf("entry %d retained relative ttl: %#v", idx, entries[idx].Request)
		}
		if entries[idx].Request.UnixSeconds == nil || *entries[idx].Request.UnixSeconds != now.Add(10*time.Second).Unix() {
			t.Fatalf("entry %d unix expiration = %#v, want %d", idx, entries[idx].Request.UnixSeconds, now.Add(10*time.Second).Unix())
		}
	}
	if entries[0].Request.Command != "SETSTR" {
		t.Fatalf("first journal command = %q, want SETSTR", entries[0].Request.Command)
	}
	if entries[2].Request.Command != "EXPIREAT" {
		t.Fatalf("expire journal command = %q, want EXPIREAT", entries[2].Request.Command)
	}

	replayedNow := now.Add(9 * time.Second)
	replayed := newTestTrie(t)
	replayed.now = func() time.Time { return replayedNow }
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.TTL("temp"); got != time.Second {
		t.Fatalf("replayed temp TTL = %s, want 1s", got)
	}
	if got := replayed.TTL("refresh"); got != time.Second {
		t.Fatalf("replayed refresh TTL = %s, want 1s", got)
	}

	replayedNow = now.Add(11 * time.Second)
	if got := replayed.GetString("temp"); got != "" {
		t.Fatalf("replayed temp after original expiry = %q, want expired", got)
	}
	if got := replayed.GetString("refresh"); got != "" {
		t.Fatalf("replayed refresh after original expiry = %q, want expired", got)
	}
}

func TestCommandJournalSkipsInvalidAndReadOnlyCommands(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	ht := newTestTrie(t)

	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "GET", Key: "missing"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETINT", Key: "counter", Value: "bad"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "PUSHSLICE", Key: "slice"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ADDSET", Key: "set"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INTERNALSET", Key: "replicated", Value: `{"key":"other","type":"string","string":"value"}`})

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("journal entries = %d, want 0", len(entries))
	}
}

func TestCommandJournalSnapshotCheckpointPreventsDoubleReplay(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "commands.journal")
	snapshotPath := filepath.Join(dir, "snapshot.json")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	ht := newTestTrie(t)

	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETINT", Key: "views", Value: "1"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "views", Value: "2"})
	if got := ht.GetCounter("views"); got != 3 {
		t.Fatalf("views before snapshot = %d, want 3", got)
	}
	if err := journal.SaveSnapshot(ht, snapshotPath); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if entries, err := readCommandJournalEntries(journalPath); err != nil || len(entries) != 1 || !entries[0].Checkpoint || entries[0].Sequence != 2 {
		t.Fatalf("entries after compact = %#v/%v, want checkpoint sequence 2", entries, err)
	}

	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "views", Value: "3"})

	loaded := newTestTrie(t)
	metadata, err := loaded.LoadSnapshotWithMetadata(snapshotPath)
	if err != nil {
		t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
	}
	if metadata.JournalSequence != 2 {
		t.Fatalf("snapshot journal sequence = %d, want 2", metadata.JournalSequence)
	}
	if got := loaded.GetCounter("views"); got != 3 {
		t.Fatalf("loaded snapshot views = %d, want 3", got)
	}

	replayJournal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(loaded, metadata.JournalSequence); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := loaded.GetCounter("views"); got != 6 {
		t.Fatalf("loaded views after replay = %d, want 6", got)
	}
}

func TestCommandJournalTailReportsCompactedBoundary(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "commands.journal")
	snapshotPath := filepath.Join(dir, "snapshot.json")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	ht := newTestTrie(t)
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETINT", Key: "views", Value: "1"})
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "views", Value: "2"})
	if err := journal.SaveSnapshot(ht, snapshotPath); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "views", Value: "3"})

	if _, err := journal.Tail(0, 0); !errors.Is(err, ErrCommandJournalCompacted) {
		t.Fatalf("Tail(0) error = %v, want ErrCommandJournalCompacted", err)
	}
	tail, err := journal.Tail(2, 0)
	if err != nil {
		t.Fatalf("Tail(2) error = %v", err)
	}
	if tail.LastSequence != 3 || tail.CompactedThrough != 2 || len(tail.Entries) != 1 {
		t.Fatalf("tail after compaction = %#v, want sequence 3 after compacted 2", tail)
	}
	if entry := tail.Entries[0]; entry.Sequence != 3 || entry.Request.Command != "INC" {
		t.Fatalf("tail entry after compaction = %#v, want sequence 3 INC", entry)
	}
}

func TestCommandJournalCloseIsIdempotentAndRejectsWork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	ht := newTestTrie(t)
	response := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"})
	if response.OK || response.Message != ErrCommandJournalClosed.Error() {
		t.Fatalf("ExecuteCommand after close = %#v, want closed error", response)
	}
	if ht.Exists("name") {
		t.Fatal("ExecuteCommand after close mutated trie")
	}
	if _, err := journal.Replay(ht, 0); !errors.Is(err, ErrCommandJournalClosed) {
		t.Fatalf("Replay after close error = %v, want ErrCommandJournalClosed", err)
	}
	if _, err := journal.Tail(0, 0); !errors.Is(err, ErrCommandJournalClosed) {
		t.Fatalf("Tail after close error = %v, want ErrCommandJournalClosed", err)
	}
	if err := journal.SaveSnapshot(ht, filepath.Join(t.TempDir(), "snapshot.json")); !errors.Is(err, ErrCommandJournalClosed) {
		t.Fatalf("SaveSnapshot after close error = %v, want ErrCommandJournalClosed", err)
	}
}

func TestCommandJournalIgnoresPartialTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	ht := newTestTrie(t)
	_ = journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"})

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := file.WriteString(`{"version":1`); err != nil {
		file.Close()
		t.Fatalf("WriteString() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetString("name"); got != "ivi" {
		t.Fatalf("replayed name = %q, want ivi", got)
	}

	_ = replayJournal.ExecuteCommand(replayed, CacheCommandRequest{Command: "SETSTR", Key: "city", Value: "Singapore"})
	replayedAgain := newTestTrie(t)
	replayJournalAgain, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay again) error = %v", err)
	}
	if _, err := replayJournalAgain.Replay(replayedAgain, 0); err != nil {
		t.Fatalf("Replay(again) error = %v", err)
	}
	if got := replayedAgain.GetString("city"); got != "Singapore" {
		t.Fatalf("replayed city after partial tail append = %q, want Singapore", got)
	}
}

func TestOpenCommandJournalRejectsUnsupportedVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	if err := os.WriteFile(path, []byte(`{"version":99,"sequence":1,"request":{"command":"SETSTR","key":"name","value":"ivi"}}`+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := OpenCommandJournal(path); err == nil {
		t.Fatal("OpenCommandJournal(unsupported version) error = nil, want error")
	}
}
