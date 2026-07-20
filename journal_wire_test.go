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

func TestDetachCommandJournalTailBorrowedStrings(t *testing.T) {
	tail := CommandJournalTail{
		wireFormat: CommandJournalWireFormatBinary,
		Entries: []CommandJournalRecord{{
			Request: CacheCommandRequest{Key: "key", Value: "value", Subkey: "subkey"},
		}},
	}
	before := tail.Entries[0].Request
	detachCommandJournalTailBorrowedStrings(&tail)
	after := tail.Entries[0].Request
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("detached request changed from %#v to %#v", before, after)
	}
	if unsafe.StringData(before.Key) == unsafe.StringData(after.Key) ||
		unsafe.StringData(before.Value) == unsafe.StringData(after.Value) ||
		unsafe.StringData(before.Subkey) == unsafe.StringData(after.Subkey) {
		t.Fatal("retained binary request strings still share the response buffer")
	}

	jsonTail := CommandJournalTail{wireFormat: CommandJournalWireFormatJSON, Entries: tail.Entries}
	before = jsonTail.Entries[0].Request
	detachCommandJournalTailBorrowedStrings(&jsonTail)
	if unsafe.StringData(before.Key) != unsafe.StringData(jsonTail.Entries[0].Request.Key) {
		t.Fatal("JSON request strings were unnecessarily cloned")
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
