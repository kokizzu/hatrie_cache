package hatriecache

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"hatrie_cache/internal/jsonwire"
)

func TestParseCommandJournalWireFormat(t *testing.T) {
	for _, test := range []struct {
		value string
		want  CommandJournalWireFormat
	}{
		{want: CommandJournalWireFormatBinary},
		{value: "binary", want: CommandJournalWireFormatBinary},
		{value: " bin ", want: CommandJournalWireFormatBinary},
		{value: "json", want: CommandJournalWireFormatJSON},
	} {
		got, err := ParseCommandJournalWireFormat(test.value)
		if err != nil || got != test.want {
			t.Fatalf("ParseCommandJournalWireFormat(%q) = %q/%v, want %q", test.value, got, err, test.want)
		}
	}
	if _, err := ParseCommandJournalWireFormat("protobuf"); err == nil {
		t.Fatal("ParseCommandJournalWireFormat(protobuf) error = nil")
	}
}

func TestCommandJournalRequestAcceptsBinary(t *testing.T) {
	for _, test := range []struct {
		accept string
		want   bool
	}{
		{accept: commandJournalTailBinaryContentType, want: true},
		{accept: "Application/Vnd.Hatrie-Cache.Journal-Tail; q=0.5", want: true},
		{accept: commandJournalTailBinaryContentType + ";q=0", want: false},
		{accept: commandJournalTailBinaryContentType + ";q=0.0, application/json", want: false},
		{accept: "application/json", want: false},
	} {
		if got := commandJournalRequestAcceptsBinary(test.accept); got != test.want {
			t.Fatalf("commandJournalRequestAcceptsBinary(%q) = %t, want %t", test.accept, got, test.want)
		}
	}
}

func TestCommandJournalTailBinaryRoundTripAndRejectsTrailingData(t *testing.T) {
	ttl := int64(30)
	want := CommandJournalTail{
		LastSequence:     9,
		CompactedThrough: 6,
		Limit:            2,
		HasMore:          true,
		Entries: []CommandJournalRecord{
			{Sequence: 7, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi", TTLSeconds: &ttl}},
			{Sequence: 8, Request: CacheCommandRequest{Command: "PUTMAP", Key: "profile", Pairs: Map{"city": "Singapore"}}},
		},
	}
	data, err := marshalCommandJournalTailBinary(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeCommandJournalTailBinaryResponse(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	got.wireFormat = ""
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("binary tail = %#v, want %#v", got, want)
	}
	if _, err := decodeCommandJournalTailBinaryResponse(bytes.NewReader(append(data, 0))); err == nil {
		t.Fatal("binary tail with trailing data was accepted")
	}
	if _, err := marshalCommandJournalTailBinary(CommandJournalTail{Limit: 1, Entries: make([]CommandJournalRecord, 2)}); err == nil {
		t.Fatal("binary tail with more entries than its limit was accepted")
	}
}

func TestCommandJournalTailCompactScalarPullAppliesAndReplays(t *testing.T) {
	want := CommandJournalTail{
		LastSequence: 2,
		Limit:        2,
		Entries: []CommandJournalRecord{
			{Sequence: 1, Request: CacheCommandRequest{Command: "SETINT", Key: "first", Value: "7"}},
			{Sequence: 2, Request: CacheCommandRequest{Command: "SETINT", Key: "second", Value: "9"}},
		},
	}
	data, err := marshalCommandJournalTailBinary(want)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := decodeCommandJournalTailBinaryPullResponse(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(tail.Entries) != 0 || len(tail.compactEntries) != len(want.Entries) {
		t.Fatalf("decoded tail entries = %d full/%d compact, want 0/%d", len(tail.Entries), len(tail.compactEntries), len(want.Entries))
	}

	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "compact.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	result, err := applyCommandJournalTail(trie, journal, "source", 0, tail)
	if err != nil || result.Applied != 2 || result.AppliedThrough != 2 {
		t.Fatalf("applyCommandJournalTail() = %#v/%v", result, err)
	}
	if trie.GetCounter("first") != 7 || trie.GetCounter("second") != 9 {
		t.Fatal("compact scalar tail did not apply exact counter values")
	}
	replayed := newTestTrie(t)
	if _, err := journal.Replay(replayed, 0); err != nil {
		t.Fatal(err)
	}
	if replayed.GetCounter("first") != 7 || replayed.GetCounter("second") != 9 {
		t.Fatal("compact scalar tail did not replay exact counter values")
	}
}

func TestCompactCommandJournalRecordStaysSmall(t *testing.T) {
	if got := unsafe.Sizeof(compactCommandJournalRecord{}); got > 48 {
		t.Fatalf("compactCommandJournalRecord size = %d, want <= 48", got)
	}
}

func TestCommandJournalTailCompactScalarPullFallsBack(t *testing.T) {
	ttl := int64(30)
	tests := []struct {
		name    string
		entries []CommandJournalRecord
	}{
		{
			name: "ttl",
			entries: []CommandJournalRecord{{
				Sequence: 1,
				Request:  CacheCommandRequest{Command: "SETINT", Key: "ttl", Value: "1", TTLSeconds: &ttl},
			}},
		},
		{
			name: "mixed families",
			entries: []CommandJournalRecord{
				{Sequence: 1, Request: CacheCommandRequest{Command: "SETINT", Key: "counter", Value: "1"}},
				{Sequence: 2, Request: CacheCommandRequest{Command: "SETSTR", Key: "string", Value: "value"}},
			},
		},
		{
			name: "structured",
			entries: []CommandJournalRecord{{
				Sequence: 1,
				Request:  CacheCommandRequest{Command: "PUTMAP", Key: "profile", Pairs: Map{"city": "Singapore"}},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := CommandJournalTail{LastSequence: uint64(len(test.entries)), Limit: len(test.entries), Entries: test.entries}
			data, err := marshalCommandJournalTailBinary(want)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeCommandJournalTailBinaryPullResponse(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			if len(got.compactEntries) != 0 || !reflect.DeepEqual(got.Entries, want.Entries) {
				t.Fatalf("fallback tail = %#v, want full entries %#v", got, want.Entries)
			}
		})
	}
}

func TestCommandJournalTailCompactScalarPullRejectsAndTruncatesSuffix(t *testing.T) {
	want := CommandJournalTail{
		LastSequence: 3,
		Limit:        3,
		Entries: []CommandJournalRecord{
			{Sequence: 1, Request: CacheCommandRequest{Command: "SETINT", Key: "first", Value: "1"}},
			{Sequence: 2, Request: CacheCommandRequest{Command: "SETINT", Key: "invalid", Value: "not-an-int"}},
			{Sequence: 3, Request: CacheCommandRequest{Command: "SETINT", Key: "suffix", Value: "3"}},
		},
	}
	data, err := marshalCommandJournalTailBinary(want)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := decodeCommandJournalTailBinaryPullResponse(bytes.NewReader(data), int64(len(data)))
	if err != nil || len(tail.compactEntries) != 3 {
		t.Fatalf("compact decode = %#v/%v", tail, err)
	}
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "compact-reject.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	result, err := applyCommandJournalTail(trie, journal, "source", 0, tail)
	if err == nil || result.Applied != 1 || result.AppliedThrough != 1 {
		t.Fatalf("applyCommandJournalTail() = %#v/%v, want one applied record", result, err)
	}
	if trie.GetCounter("first") != 1 || trie.Exists("invalid") || trie.Exists("suffix") {
		t.Fatal("compact rejection did not preserve only the successful prefix")
	}
	local, err := journal.Tail(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(local.Entries) != 1 || local.Entries[0].Request.Key != "first" {
		t.Fatalf("local journal entries = %#v, want only first", local.Entries)
	}
}

func TestCommandJournalTailCompactStringOwnsStoredValue(t *testing.T) {
	want := CommandJournalTail{
		LastSequence: 1,
		Limit:        1,
		Entries: []CommandJournalRecord{{
			Sequence: 1,
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "compact-owned-value"},
		}},
	}
	data, err := marshalCommandJournalTailBinary(want)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := decodeCommandJournalTailBinaryPullResponse(bytes.NewReader(data), int64(len(data)))
	if err != nil || len(tail.compactEntries) != 1 {
		t.Fatalf("compact decode = %#v/%v", tail, err)
	}
	borrowed := tail.compactEntries[0].Value
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "compact-string.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err := applyCommandJournalTail(trie, journal, "source", 0, tail); err != nil {
		t.Fatal(err)
	}
	stored := trie.GetString("name")
	if stored != borrowed || unsafe.StringData(stored) == unsafe.StringData(borrowed) {
		t.Fatal("compact SETSTR stored value still borrows the response body")
	}
}

func TestCommandJournalTailCompactSyncFailureDoesNotApply(t *testing.T) {
	want := CommandJournalTail{
		LastSequence: 1,
		Limit:        1,
		Entries: []CommandJournalRecord{{
			Sequence: 1,
			Request:  CacheCommandRequest{Command: "SETINT", Key: "unapplied", Value: "7"},
		}},
	}
	data, err := marshalCommandJournalTailBinary(want)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := decodeCommandJournalTailBinaryPullResponse(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "compact-sync.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	journal.syncHook = func() error { return errors.New("injected compact sync failure") }
	result, err := applyCommandJournalTail(trie, journal, "source", 0, tail)
	if err == nil || result.Applied != 0 || !strings.Contains(err.Error(), "injected compact sync failure") {
		t.Fatalf("applyCommandJournalTail() = %#v/%v, want sync failure", result, err)
	}
	if trie.Exists("unapplied") || journal.Sequence() != 0 {
		t.Fatal("compact sync failure changed cache state or journal sequence")
	}
	local, err := journal.Tail(0, 10)
	if err != nil || len(local.Entries) != 0 {
		t.Fatalf("local journal tail = %#v/%v, want empty", local, err)
	}
}

func TestDetachCommandJournalTailBorrowedStrings(t *testing.T) {
	trie := newTestTrie(t)
	tail := CommandJournalTail{
		wireFormat: CommandJournalWireFormatBinary,
		Entries: []CommandJournalRecord{{
			Request: CacheCommandRequest{Command: "SETINT", Key: "key", Value: "42"},
		}},
	}
	before := tail.Entries[0].Request
	detachCommandJournalTailBorrowedStrings(trie, &tail, nil)
	after := tail.Entries[0].Request
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("detached request changed from %#v to %#v", before, after)
	}
	if unsafe.StringData(before.Key) != unsafe.StringData(after.Key) ||
		unsafe.StringData(before.Value) != unsafe.StringData(after.Value) {
		t.Fatal("plain SETINT fields were unnecessarily cloned")
	}

	jsonTail := CommandJournalTail{wireFormat: CommandJournalWireFormatJSON, Entries: tail.Entries}
	before = jsonTail.Entries[0].Request
	detachCommandJournalTailBorrowedStrings(trie, &jsonTail, nil)
	if unsafe.StringData(before.Key) != unsafe.StringData(jsonTail.Entries[0].Request.Key) {
		t.Fatal("JSON request strings were unnecessarily cloned")
	}

	stringTail := CommandJournalTail{
		wireFormat: CommandJournalWireFormatBinary,
		Entries: []CommandJournalRecord{{
			Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "value"},
		}},
	}
	before = stringTail.Entries[0].Request
	detachCommandJournalTailBorrowedStrings(trie, &stringTail, nil)
	after = stringTail.Entries[0].Request
	if unsafe.StringData(before.Key) != unsafe.StringData(after.Key) ||
		unsafe.StringData(before.Value) == unsafe.StringData(after.Value) {
		t.Fatal("SETSTR must own only its retained value under default state")
	}

	expiresAt := int64(1 << 62)
	ttlTail := CommandJournalTail{
		wireFormat: CommandJournalWireFormatBinary,
		Entries: []CommandJournalRecord{{
			Request: CacheCommandRequest{Command: "SETINT", Key: "ttl", Value: "7", UnixSeconds: &expiresAt},
		}},
	}
	before = ttlTail.Entries[0].Request
	detachCommandJournalTailBorrowedStrings(trie, &ttlTail, nil)
	after = ttlTail.Entries[0].Request
	if unsafe.StringData(before.Key) == unsafe.StringData(after.Key) ||
		unsafe.StringData(before.Value) != unsafe.StringData(after.Value) {
		t.Fatal("TTL SETINT must own only its retained expiration key")
	}

	if err := trie.ConfigureKeyStats(KeyStatsModeFull, 0); err != nil {
		t.Fatal(err)
	}
	statsTail := CommandJournalTail{
		wireFormat: CommandJournalWireFormatBinary,
		Entries: []CommandJournalRecord{{
			Request: CacheCommandRequest{Command: "SETINT", Key: "tracked", Value: "9"},
		}},
	}
	before = statsTail.Entries[0].Request
	detachCommandJournalTailBorrowedStrings(trie, &statsTail, nil)
	after = statsTail.Entries[0].Request
	if unsafe.StringData(before.Key) == unsafe.StringData(after.Key) ||
		unsafe.StringData(before.Value) != unsafe.StringData(after.Value) {
		t.Fatal("key stats SETINT must own only its retained telemetry key")
	}

	dirtyTail := CommandJournalTail{
		wireFormat: CommandJournalWireFormatBinary,
		Entries: []CommandJournalRecord{{
			Request: CacheCommandRequest{Command: "SETINT", Key: "dirty", Value: "11"},
		}},
	}
	before = dirtyTail.Entries[0].Request
	detachCommandJournalTailBorrowedStrings(newTestTrie(t), &dirtyTail, NewLevelDBDirtyTracker())
	after = dirtyTail.Entries[0].Request
	if unsafe.StringData(before.Key) == unsafe.StringData(after.Key) ||
		unsafe.StringData(before.Value) != unsafe.StringData(after.Value) {
		t.Fatal("dirty tracking SETINT must own only its retained dirty key")
	}
}

func TestMonitoringJournalTailNegotiatesBinaryAndJSON(t *testing.T) {
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "source.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "binary"}); !response.OK {
		t.Fatal(response.Message)
	}
	server := httptest.NewServer(NewMonitoringHandler(trie, MonitoringOptions{Journal: journal}).Handler())
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL+"/api/journal", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept", commandJournalTailBinaryContentType)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if got := response.Header.Get("Content-Type"); got != commandJournalTailBinaryContentType {
		response.Body.Close()
		t.Fatalf("binary content type = %q", got)
	}
	binaryTail, err := decodeCommandJournalTailBinaryResponse(response.Body)
	response.Body.Close()
	if err != nil || len(binaryTail.Entries) != 1 || binaryTail.Entries[0].Request.Value != "binary" {
		t.Fatalf("binary tail = %#v/%v", binaryTail, err)
	}

	response, err = server.Client().Get(server.URL + "/api/journal")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
		response.Body.Close()
		t.Fatalf("JSON content type = %q", response.Header.Get("Content-Type"))
	}
	jsonTail, err := decodeCommandJournalTailResponse(response.Body)
	response.Body.Close()
	if err != nil || len(jsonTail.Entries) != 1 || jsonTail.Entries[0].Request.Value != "binary" {
		t.Fatalf("JSON tail = %#v/%v", jsonTail, err)
	}
}

func TestPullCommandJournalDefaultsBinaryAndFallsBackToJSON(t *testing.T) {
	sourceTrie := newTestTrie(t)
	sourceJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "source.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer sourceJournal.Close()
	if response := sourceJournal.ExecuteCommand(sourceTrie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "current"}); !response.OK {
		t.Fatal(response.Message)
	}
	current := httptest.NewServer(NewMonitoringHandler(sourceTrie, MonitoringOptions{Journal: sourceJournal}).Handler())
	defer current.Close()
	target := newTestTrie(t)
	targetJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "target.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer targetJournal.Close()
	result, err := PullCommandJournal(context.Background(), target, targetJournal, CommandJournalPullOptions{Source: current.URL, Client: current.Client()})
	if err != nil || result.WireFormat != string(CommandJournalWireFormatBinary) || target.GetString("name") != "current" {
		t.Fatalf("binary pull = %#v/%v value=%q", result, err, target.GetString("name"))
	}

	tail, err := sourceJournal.Tail(0, DefaultCommandJournalTailLimit)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := jsonwire.Marshal(tail)
	if err != nil {
		t.Fatal(err)
	}
	var accept string
	legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accept = r.Header.Get("Accept")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer legacy.Close()
	legacyTarget := newTestTrie(t)
	legacyJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "legacy.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer legacyJournal.Close()
	result, err = PullCommandJournal(context.Background(), legacyTarget, legacyJournal, CommandJournalPullOptions{Source: legacy.URL, Client: legacy.Client()})
	if err != nil || result.WireFormat != string(CommandJournalWireFormatJSON) || !strings.Contains(accept, commandJournalTailBinaryContentType) || legacyTarget.GetString("name") != "current" {
		t.Fatalf("legacy pull = %#v/%v accept=%q value=%q", result, err, accept, legacyTarget.GetString("name"))
	}
}

func TestPullCommandJournalCanForceJSON(t *testing.T) {
	tail := CommandJournalTail{Entries: []CommandJournalRecord{}}
	payload, err := jsonwire.Marshal(tail)
	if err != nil {
		t.Fatal(err)
	}
	var accept string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accept = r.Header.Get("Accept")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	target := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "target.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err := PullCommandJournal(context.Background(), target, journal, CommandJournalPullOptions{
		Source: server.URL, Client: server.Client(), WireFormat: CommandJournalWireFormatJSON,
	}); err != nil {
		t.Fatal(err)
	}
	if accept != "application/json" {
		t.Fatalf("forced JSON Accept = %q", accept)
	}
}

func TestPullCommandJournalRejectsInvalidWireFormat(t *testing.T) {
	target := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "target.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	_, err = PullCommandJournal(context.Background(), target, journal, CommandJournalPullOptions{
		Source:     "http://unused",
		WireFormat: CommandJournalWireFormat("protobuf"),
	})
	var pullErr *CommandJournalPullError
	if !errors.As(err, &pullErr) || pullErr.Status != http.StatusBadRequest {
		t.Fatalf("invalid wire format error = %#v, want HTTP 400", err)
	}
}
