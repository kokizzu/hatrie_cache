package hatriecache

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func writeCommandJournalTestEntries(t *testing.T, path string, entries ...commandJournalEntry) {
	t.Helper()
	data := []byte{}
	for _, entry := range entries {
		payload, err := json.Marshal(entry)
		if err != nil {
			t.Fatalf("Marshal(commandJournalEntry) error = %v", err)
		}
		data = append(data, payload...)
		data = append(data, '\n')
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}

func readCommandJournalEntries(path string) ([]commandJournalEntry, error) {
	var entries []commandJournalEntry
	if _, err := scanCommandJournalEntries(path, func(entry commandJournalEntry) error {
		entries = append(entries, entry)
		return nil
	}); err != nil {
		return nil, err
	}
	return entries, nil
}

func TestParseCommandJournalFormat(t *testing.T) {
	tests := []struct {
		value string
		want  CommandJournalFormat
	}{
		{value: "", want: CommandJournalFormatBinary},
		{value: "binary", want: CommandJournalFormatBinary},
		{value: "bin", want: CommandJournalFormatBinary},
		{value: " json ", want: CommandJournalFormatJSON},
	}
	for _, tt := range tests {
		got, err := ParseCommandJournalFormat(tt.value)
		if err != nil {
			t.Fatalf("ParseCommandJournalFormat(%q) error = %v", tt.value, err)
		}
		if got != tt.want {
			t.Fatalf("ParseCommandJournalFormat(%q) = %q, want %q", tt.value, got, tt.want)
		}
	}

	if _, err := ParseCommandJournalFormat("msgpack"); err == nil {
		t.Fatal("ParseCommandJournalFormat(msgpack) error = nil, want error")
	}
}

func TestOpenCommandJournalRejectsInvalidGroupCommitOptions(t *testing.T) {
	for _, test := range []struct {
		name    string
		options CommandJournalOptions
	}{
		{
			name: "negative window",
			options: CommandJournalOptions{
				GroupCommitWindow:   -time.Nanosecond,
				GroupCommitMaxBatch: DefaultJournalGroupCommitMaxBatch,
			},
		},
		{
			name: "zero batch",
			options: CommandJournalOptions{
				GroupCommitMaxBatch: 0,
			},
		},
		{
			name: "huge batch",
			options: CommandJournalOptions{
				GroupCommitMaxBatch: MaxJournalGroupCommitBatch + 1,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), test.options); err == nil {
				t.Fatalf("OpenCommandJournalWithOptions(%#v) error = nil, want validation error", test.options)
			}
		})
	}
}

func TestCommandJournalGroupCommitCoalescesConcurrentDurableWrites(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitWindow:   20 * time.Millisecond,
		GroupCommitMaxBatch: 64,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	var syncs atomic.Int64
	journal.mu.Lock()
	journal.syncHook = func() error {
		syncs.Add(1)
		return journal.file.Sync()
	}
	journal.mu.Unlock()

	ht := newTestTrie(t)
	const commands = 16
	start := make(chan struct{})
	responses := make(chan CacheCommandResponse, commands)
	var workers sync.WaitGroup
	workers.Add(commands)
	for idx := 0; idx < commands; idx++ {
		go func(idx int) {
			defer workers.Done()
			<-start
			responses <- journal.ExecuteCommand(ht, CacheCommandRequest{
				Command: "SETSTR",
				Key:     fmt.Sprintf("key:%d", idx),
				Value:   "value",
			})
		}(idx)
	}
	close(start)
	workers.Wait()
	close(responses)
	for response := range responses {
		if !response.OK {
			t.Fatalf("ExecuteCommand() = %#v, want ok", response)
		}
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("journal fsync calls = %d, want one group commit", got)
	}
	if got := ht.Size(); got != commands {
		t.Fatalf("trie size = %d, want %d", got, commands)
	}
	if tail, err := journal.Tail(0, commands); err != nil || len(tail.Entries) != commands {
		t.Fatalf("Tail() = %#v/%v, want %d durable entries", tail, err, commands)
	}
}

func TestCommandJournalGroupCommitAcknowledgesOnlyAfterSync(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitWindow:   time.Microsecond,
		GroupCommitMaxBatch: 64,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	journal.mu.Lock()
	journal.syncHook = func() error {
		close(syncStarted)
		<-releaseSync
		return journal.file.Sync()
	}
	journal.mu.Unlock()

	ht := newTestTrie(t)
	response := make(chan CacheCommandResponse, 1)
	go func() {
		response <- journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "key", Value: "value"})
	}()
	<-syncStarted
	select {
	case got := <-response:
		close(releaseSync)
		t.Fatalf("ExecuteCommand() returned before fsync completed: %#v", got)
	case <-time.After(20 * time.Millisecond):
	}
	if got := ht.GetString("key"); got != "" {
		close(releaseSync)
		t.Fatalf("GetString(key) before fsync = %q, want command not applied", got)
	}
	close(releaseSync)
	if got := <-response; !got.OK {
		t.Fatalf("ExecuteCommand() after fsync = %#v, want ok", got)
	}
	if got := ht.GetString("key"); got != "value" {
		t.Fatalf("GetString(key) after fsync = %q, want value", got)
	}
}

func TestCommandJournalGroupCommitReappendsSuffixAfterRejectedCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	ht := newTestTrie(t)
	requests := []CacheCommandRequest{
		{Command: "SETSTR", Key: "first", Value: "one"},
		{Command: "SETINT", Key: "rejected", Value: "not-an-integer"},
		{Command: "SETSTR", Key: "last", Value: "three"},
	}
	jobs := make([]*commandJournalJob, len(requests))
	for idx, request := range requests {
		jobs[idx] = &commandJournalJob{
			trie:           ht,
			request:        request,
			journalRequest: normalizeJournalRequest(request, time.Unix(1_000, 0)),
			result:         make(chan CacheCommandResponse, 1),
		}
	}
	journal.processGroupCommit(jobs)

	if response := <-jobs[0].result; !response.OK {
		t.Fatalf("first response = %#v, want ok", response)
	}
	if response := <-jobs[1].result; response.OK {
		t.Fatalf("rejected response = %#v, want error", response)
	}
	if response := <-jobs[2].result; !response.OK {
		t.Fatalf("last response = %#v, want ok", response)
	}
	if got := ht.GetString("first"); got != "one" {
		t.Fatalf("GetString(first) = %q, want one", got)
	}
	if got := ht.GetString("rejected"); got != "" {
		t.Fatalf("GetString(rejected) = %q, want missing", got)
	}
	if got := ht.GetString("last"); got != "three" {
		t.Fatalf("GetString(last) = %q, want three", got)
	}
	tail, err := journal.Tail(0, 10)
	if err != nil {
		t.Fatalf("Tail() error = %v", err)
	}
	if len(tail.Entries) != 2 || tail.Entries[0].Request.Key != "first" || tail.Entries[1].Request.Key != "last" {
		t.Fatalf("Tail().Entries = %#v, want first and re-appended last", tail.Entries)
	}
	if tail.Entries[0].Sequence != 1 || tail.Entries[1].Sequence != 2 {
		t.Fatalf("Tail() sequences = %d/%d, want contiguous 1/2", tail.Entries[0].Sequence, tail.Entries[1].Sequence)
	}

	replayed := newTestTrie(t)
	if _, err := journal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if replayed.GetString("first") != "one" || replayed.GetString("last") != "three" || replayed.GetString("rejected") != "" {
		t.Fatal("Replay() did not preserve successful group commit commands")
	}
}

func TestCommandJournalGroupCommitSyncFailureDoesNotApply(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              DefaultCommandJournalFormat,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	var syncs int
	journal.syncHook = func() error {
		syncs++
		if syncs == 1 {
			return errors.New("injected fsync failure")
		}
		return journal.file.Sync()
	}
	ht := newTestTrie(t)
	request := CacheCommandRequest{Command: "SETSTR", Key: "key", Value: "value"}
	job := &commandJournalJob{
		trie:           ht,
		request:        request,
		journalRequest: normalizeJournalRequest(request, time.Unix(1_001, 0)),
		result:         make(chan CacheCommandResponse, 1),
	}
	journal.processGroupCommit([]*commandJournalJob{job})

	if response := <-job.result; response.OK || !strings.Contains(response.Message, "injected fsync failure") {
		t.Fatalf("group commit response = %#v, want injected fsync failure", response)
	}
	if got := ht.GetString("key"); got != "" {
		t.Fatalf("GetString(key) = %q, want command unapplied", got)
	}
	if sequence := journal.Sequence(); sequence != 0 {
		t.Fatalf("Sequence() = %d, want rollback to zero", sequence)
	}
	if tail, err := journal.Tail(0, 10); err != nil || len(tail.Entries) != 0 {
		t.Fatalf("Tail() = %#v/%v, want empty journal after fsync failure", tail, err)
	}
}

func TestCommandJournalDefaultWritesBinaryAndReadsLegacyJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	writeCommandJournalTestEntries(t, path, commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 1,
		Request:  CacheCommandRequest{Command: "SETSTR", Key: "legacy", Value: "json"},
	})

	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "fresh", Value: "binary"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !bytes.HasPrefix(raw, []byte("{")) {
		t.Fatalf("journal prefix = %q, want legacy JSON first record", raw[:1])
	}
	if !bytes.Contains(raw, commandJournalBinaryMagic) {
		t.Fatalf("journal does not contain binary record magic")
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("journal entries = %d, want 2", len(entries))
	}
	if entries[1].Sequence != 2 || entries[1].Request.Key != "fresh" {
		t.Fatalf("second journal entry = %#v, want fresh sequence 2", entries[1])
	}
}

func TestCommandJournalJSONFormatWritesPreviousLayout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithFormat(path, CommandJournalFormatJSON)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat(json) error = %v", err)
	}
	defer journal.Close()

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !bytes.HasPrefix(raw, []byte("{")) || !bytes.HasSuffix(raw, []byte("\n")) {
		t.Fatalf("journal raw record = %q, want JSON line", raw)
	}
	if !bytes.Contains(raw, []byte(`"version":1`)) {
		t.Fatalf("journal raw record = %q, want previous JSON version 1 layout", raw)
	}
	if commandJournalRecordIsBinary(raw) {
		t.Fatal("JSON journal record detected as binary")
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Request.Key != "name" {
		t.Fatalf("journal entries = %#v, want name entry", entries)
	}
}

func TestCommandJournalJSONFormatRejectsOversizedRecordBeforeWrite(t *testing.T) {
	_, err := marshalCommandJournalEntryJSONLimited(commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 1,
		Request: CacheCommandRequest{
			Command: "SETSTR",
			Key:     "large",
			Value:   strings.Repeat("x", 64),
		},
	}, 32)
	if !errors.Is(err, errCommandJournalJSONRecordTooLarge) {
		t.Fatalf("marshalCommandJournalEntryJSONLimited(oversized) error = %v, want record too large", err)
	}
}

func TestCommandJournalBinaryPreservesDynamicValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	request := CacheCommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs: Map{
			"nested": Map{
				"age":  json.Number("32"),
				"tags": Slice{"alpha", "beta"},
			},
		},
	}
	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, request); !got.OK {
		t.Fatalf("journaled PUTMAP response = %#v, want ok", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("journal entries = %d, want 1", len(entries))
	}
	want := Map{"age": json.Number("32"), "tags": Slice{"alpha", "beta"}}
	if got := entries[0].Request.Pairs["nested"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded nested value = %#v, want %#v", got, want)
	}
}

func TestCommandJournalBinaryUsesCompactDynamicPayloadWhenSmaller(t *testing.T) {
	entry := commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 1,
		Request: CacheCommandRequest{
			Command: "PUTMAP",
			Key:     "profile",
			Pairs: Map{
				"notes": Slice{
					strings.Repeat("line\nquoted\"value", 8),
					strings.Repeat("tab\tvalue\\path", 8),
				},
			},
		},
	}
	jsonValues, jsonPairs, err := marshalCommandJournalRequestDynamicFields(entry.Request)
	if err != nil {
		t.Fatalf("marshalCommandJournalRequestDynamicFields() error = %v", err)
	}
	values, pairs, err := marshalCommandJournalRequestBinaryDynamicFields(entry.Request)
	if err != nil {
		t.Fatalf("marshalCommandJournalRequestBinaryDynamicFields() error = %v", err)
	}
	if len(values) != len(jsonValues) {
		t.Fatalf("binary values len = %d, want unchanged empty JSON values len %d", len(values), len(jsonValues))
	}
	if !snapshotValueDataIsBinary(pairs) {
		t.Fatalf("pairs payload header = % x, want compact binary value payload", pairs[:shortHeaderLen(pairs)])
	}
	if len(pairs) >= len(jsonPairs) {
		t.Fatalf("compact pairs payload len = %d, want smaller than JSON len %d", len(pairs), len(jsonPairs))
	}

	raw, err := marshalCommandJournalEntry(entry, CommandJournalFormatBinary)
	if err != nil {
		t.Fatalf("marshalCommandJournalEntry(binary) error = %v", err)
	}
	decoded, err := decodeCommandJournalEntry(raw)
	if err != nil {
		t.Fatalf("decodeCommandJournalEntry(binary compact dynamic) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, entry) {
		t.Fatalf("decoded compact dynamic entry = %#v, want %#v", decoded, entry)
	}
}

func TestCommandJournalBinaryReadsLegacyJSONDynamicPayload(t *testing.T) {
	entry := commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 1,
		Request: CacheCommandRequest{
			Command: "PUTMAP",
			Key:     "profile",
			Pairs: Map{
				"nested": Map{"age": json.Number("32"), "tags": Slice{"alpha", "beta"}},
			},
		},
	}
	values, pairs, err := marshalCommandJournalRequestDynamicFields(entry.Request)
	if err != nil {
		t.Fatalf("marshalCommandJournalRequestDynamicFields() error = %v", err)
	}
	capacity, err := commandJournalEntryBinaryPayloadCapacity(entry, len(values), len(pairs))
	if err != nil {
		t.Fatalf("commandJournalEntryBinaryPayloadCapacity() error = %v", err)
	}
	payload := newBinaryFieldWriter(nil, capacity)
	payload.writeUvarint(commandJournalVersion)
	payload.writeUvarint(entry.Sequence)
	payload.writeBool(entry.Checkpoint)
	if err := writeCommandJournalRequestBinaryFields(&payload, entry.Request, values, pairs); err != nil {
		t.Fatalf("writeCommandJournalRequestBinaryFields() error = %v", err)
	}
	record := newBinaryFieldWriter(commandJournalBinaryMagic, commandJournalBinaryRecordCapacity(len(payload.bytes())))
	record.writeBytes(payload.bytes())

	decoded, err := decodeCommandJournalEntry(record.bytes())
	if err != nil {
		t.Fatalf("decodeCommandJournalEntry(legacy binary dynamic JSON) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, entry) {
		t.Fatalf("decoded legacy binary dynamic entry = %#v, want %#v", decoded, entry)
	}
}

func TestCommandJournalBinaryPreallocatesLargeRecord(t *testing.T) {
	priority := int64(42)
	ttl := int64(60)
	unix := time.Unix(5000, 0).Unix()
	entry := commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 9,
		Request: CacheCommandRequest{
			Command:     "INTERNALSET",
			Key:         "session:large",
			Value:       `{"type":"string","string":"` + strings.Repeat("active-user-", 4096) + `"}`,
			Subkey:      "metadata",
			Priority:    &priority,
			TTLSeconds:  &ttl,
			UnixSeconds: &unix,
			Values:      Slice{"alpha", json.Number("7"), Map{"nested": "value"}},
			Pairs:       Map{"name": "ivi", "role": "admin"},
		},
	}

	payload, err := marshalCommandJournalEntryBinaryPayload(entry)
	if err != nil {
		t.Fatalf("marshalCommandJournalEntryBinaryPayload() error = %v", err)
	}
	if cap(payload) != len(payload) {
		t.Fatalf("binary journal payload cap = %d, want exact len %d", cap(payload), len(payload))
	}
	record, err := marshalCommandJournalEntryBinary(entry)
	if err != nil {
		t.Fatalf("marshalCommandJournalEntryBinary() error = %v", err)
	}
	if cap(record) != len(record) {
		t.Fatalf("binary journal record cap = %d, want exact len %d", cap(record), len(record))
	}
	decoded, err := decodeCommandJournalEntry(record)
	if err != nil {
		t.Fatalf("decodeCommandJournalEntry(preallocated binary) error = %v", err)
	}
	if !reflect.DeepEqual(decoded, entry) {
		t.Fatalf("decoded preallocated entry = %#v, want %#v", decoded, entry)
	}
	if _, err := commandJournalEntryBinaryPayloadCapacity(entry, maxCommandJournalBinaryRecordBytes, 0); !errors.Is(err, errCommandJournalBinaryRecordTooLarge) {
		t.Fatalf("commandJournalEntryBinaryPayloadCapacity(oversized) error = %v, want record too large", err)
	}
}

func TestCommandJournalBinaryEntryReaderDecodesPayloadAndCountsBytes(t *testing.T) {
	want := commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 7,
		Request: CacheCommandRequest{
			Command: "PUTMAP",
			Key:     "profile",
			Pairs:   Map{"name": "ivi"},
		},
	}
	raw, err := marshalCommandJournalEntry(want, CommandJournalFormatBinary)
	if err != nil {
		t.Fatalf("marshalCommandJournalEntry(binary) error = %v", err)
	}

	got, bytesRead, complete, err := readCommandJournalEntry(bufio.NewReader(bytes.NewReader(raw)))
	if err != nil || !complete {
		t.Fatalf("readCommandJournalEntry(binary) = %#v/%d/%v/%v, want complete entry", got, bytesRead, complete, err)
	}
	if bytesRead != len(raw) {
		t.Fatalf("binary journal bytesRead = %d, want %d", bytesRead, len(raw))
	}
	if got.Version != want.Version || got.Sequence != want.Sequence || !reflect.DeepEqual(got.Request, want.Request) {
		t.Fatalf("binary journal entry = %#v, want %#v", got, want)
	}
}

func TestCommandJournalBinaryEntryReaderPreservesEntriesWhenReusingPayloadBuffer(t *testing.T) {
	first := commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 1,
		Request:  CacheCommandRequest{Command: "SETSTR", Key: "first", Value: strings.Repeat("x", 128)},
	}
	firstRaw, err := marshalCommandJournalEntry(first, CommandJournalFormatBinary)
	if err != nil {
		t.Fatalf("marshalCommandJournalEntry(first) error = %v", err)
	}
	second := commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 2,
		Request:  CacheCommandRequest{Command: "SETSTR", Key: "second", Value: "y"},
	}
	secondRaw, err := marshalCommandJournalEntry(second, CommandJournalFormatBinary)
	if err != nil {
		t.Fatalf("marshalCommandJournalEntry(second) error = %v", err)
	}

	var buffer commandJournalReadBuffer
	reader := bufio.NewReader(bytes.NewReader(append(firstRaw, secondRaw...)))
	gotFirst, firstBytes, firstComplete, err := readCommandJournalEntryBuffered(reader, &buffer)
	if err != nil || !firstComplete {
		t.Fatalf("read first binary entry = %#v/%d/%v/%v, want complete entry", gotFirst, firstBytes, firstComplete, err)
	}
	gotSecond, secondBytes, secondComplete, err := readCommandJournalEntryBuffered(reader, &buffer)
	if err != nil || !secondComplete {
		t.Fatalf("read second binary entry = %#v/%d/%v/%v, want complete entry", gotSecond, secondBytes, secondComplete, err)
	}
	if firstBytes != len(firstRaw) || secondBytes != len(secondRaw) {
		t.Fatalf("binary journal bytes = %d/%d, want %d/%d", firstBytes, secondBytes, len(firstRaw), len(secondRaw))
	}
	if !reflect.DeepEqual(gotFirst, first) || !reflect.DeepEqual(gotSecond, second) {
		t.Fatalf("binary journal entries = %#v/%#v, want %#v/%#v", gotFirst, gotSecond, first, second)
	}
}

func TestCommandJournalPayloadBufferDoesNotRetainOversizedPayload(t *testing.T) {
	var buffer commandJournalReadBuffer
	small := commandJournalPayloadBuffer(&buffer, 32)
	if cap(buffer.payload) != cap(small) || cap(buffer.payload) == 0 {
		t.Fatalf("small payload buffer cap = %d/%d, want retained reusable buffer", cap(buffer.payload), cap(small))
	}
	large := commandJournalPayloadBuffer(&buffer, maxCommandJournalReusablePayloadBufferBytes+1)
	if len(large) != maxCommandJournalReusablePayloadBufferBytes+1 {
		t.Fatalf("large payload len = %d, want %d", len(large), maxCommandJournalReusablePayloadBufferBytes+1)
	}
	if cap(buffer.payload) != cap(small) {
		t.Fatalf("retained payload buffer cap = %d after oversized read, want small cap %d", cap(buffer.payload), cap(small))
	}

	buffer.payload = make([]byte, 8, maxCommandJournalReusablePayloadBufferBytes+1)
	shrunk := commandJournalPayloadBuffer(&buffer, 16)
	if len(shrunk) != 16 {
		t.Fatalf("shrunk payload len = %d, want 16", len(shrunk))
	}
	if cap(buffer.payload) > maxCommandJournalReusablePayloadBufferBytes {
		t.Fatalf("payload buffer retained oversized cap = %d", cap(buffer.payload))
	}
}

func TestCommandJournalIgnoresPartialBinaryTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(before) error = %v", err)
	}
	partial, err := marshalCommandJournalEntry(commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 2,
		Request:  CacheCommandRequest{Command: "SETSTR", Key: "partial", Value: strings.Repeat("x", 32)},
	}, CommandJournalFormatBinary)
	if err != nil {
		t.Fatalf("marshalCommandJournalEntry(binary) error = %v", err)
	}
	if err := os.WriteFile(path, append(before, partial[:len(partial)-2]...), 0o600); err != nil {
		t.Fatalf("WriteFile(partial) error = %v", err)
	}

	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(partial) error = %v", err)
	}
	defer replayJournal.Close()
	if got := replayJournal.Sequence(); got != 1 {
		t.Fatalf("Sequence() after partial tail = %d, want 1", got)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(after) error = %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("journal after partial tail truncate length = %d, want %d", len(after), len(before))
	}
}

func TestCommandJournalRejectsOversizedBinaryRecordBeforeAllocation(t *testing.T) {
	writer := newBinaryFieldWriter(commandJournalBinaryMagic, len(commandJournalBinaryMagic)+binaryFieldMaxVarintLen64)
	writer.writeUvarint(maxCommandJournalBinaryRecordBytes + 1)

	_, _, _, err := readCommandJournalEntry(bufio.NewReader(bytes.NewReader(writer.bytes())))
	if !errors.Is(err, errCommandJournalBinaryRecordTooLarge) {
		t.Fatalf("readCommandJournalEntry(oversized binary) error = %v, want record too large", err)
	}
}

func TestCommandJournalRejectsOversizedJSONRecordBeforeAllocation(t *testing.T) {
	reader := bufio.NewReaderSize(strings.NewReader(strings.Repeat("x", 33)+"\n"), 8)
	_, bytesRead, complete, err := readCommandJournalJSONRecordLimited(reader, 32)
	if !errors.Is(err, errCommandJournalJSONRecordTooLarge) {
		t.Fatalf("readCommandJournalJSONRecordLimited(oversized) error = %v, want record too large", err)
	}
	if complete {
		t.Fatal("readCommandJournalJSONRecordLimited(oversized) complete = true, want false")
	}
	if bytesRead <= 32 {
		t.Fatalf("readCommandJournalJSONRecordLimited(oversized) bytesRead = %d, want over limit", bytesRead)
	}
}

func TestCommandJournalJSONRecordReaderIgnoresPartialTail(t *testing.T) {
	reader := bufio.NewReaderSize(strings.NewReader(`{"version":1`), 4)
	record, bytesRead, complete, err := readCommandJournalJSONRecordLimited(reader, 32)
	if err != nil || complete || bytesRead != 0 || record != nil {
		t.Fatalf("readCommandJournalJSONRecordLimited(partial) = %q/%d/%v/%v, want incomplete nil record", record, bytesRead, complete, err)
	}
}

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

func TestCommandJournalReplayScansLargeHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	entries := []commandJournalEntry{
		{
			Version:  commandJournalVersion,
			Sequence: 1,
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "large", Value: strings.Repeat("x", 70*1024)},
		},
	}
	for sequence := uint64(2); sequence <= 96; sequence++ {
		entries = append(entries, commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: sequence,
			Request:  CacheCommandRequest{Command: "INC", Key: "count"},
		})
	}
	entries = append(entries, commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 97,
		Request:  CacheCommandRequest{Command: "SETSTR", Key: "last", Value: "ok"},
	})
	writeCommandJournalTestEntries(t, path, entries...)

	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	if got := journal.Sequence(); got != uint64(len(entries)) {
		t.Fatalf("Sequence() after open = %d, want %d", got, len(entries))
	}

	replayed := newTestTrie(t)
	maxSequence, err := journal.Replay(replayed, 0)
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if maxSequence != uint64(len(entries)) {
		t.Fatalf("Replay() max sequence = %d, want %d", maxSequence, len(entries))
	}
	if got := replayed.GetString("large"); got != entries[0].Request.Value {
		t.Fatalf("replayed large value length = %d, want %d", len(got), len(entries[0].Request.Value))
	}
	if got := replayed.GetCounter("count"); got != 95 {
		t.Fatalf("replayed count = %d, want 95", got)
	}
	if got := replayed.GetString("last"); got != "ok" {
		t.Fatalf("replayed last = %q, want ok", got)
	}
}

func TestCommandJournalReplayRejectsFailedEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	writeCommandJournalTestEntries(t, path,
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 1,
			Request:  CacheCommandRequest{Command: "SETINT", Key: "max", Value: "2147483647"},
		},
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 2,
			Request:  CacheCommandRequest{Command: "INC", Key: "max", Value: "1"},
		},
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 3,
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "after", Value: "bad"},
		},
	)
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	replayed := newTestTrie(t)

	maxSequence, err := journal.Replay(replayed, 0)
	if err == nil {
		t.Fatal("Replay(failed entry) error = nil, want replay error")
	}
	if !strings.Contains(err.Error(), "entry 2 failed: counter overflow") {
		t.Fatalf("Replay(failed entry) error = %v, want sequence-specific counter overflow", err)
	}
	if maxSequence != 0 {
		t.Fatalf("Replay(failed entry) max sequence = %d, want 0", maxSequence)
	}
	if got := replayed.GetCounter("max"); got != maxCommandInt32 {
		t.Fatalf("counter before failed replay entry = %d, want max int32", got)
	}
	if got := replayed.GetString("after"); got != "" {
		t.Fatalf("entry after failed replay applied value = %q, want empty", got)
	}
}

func TestCommandJournalRejectsNonContiguousSequences(t *testing.T) {
	t.Run("gap", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "commands.journal")
		writeCommandJournalTestEntries(t, path,
			commandJournalEntry{
				Version:  commandJournalVersion,
				Sequence: 1,
				Request:  CacheCommandRequest{Command: "SETSTR", Key: "one", Value: "1"},
			},
			commandJournalEntry{
				Version:  commandJournalVersion,
				Sequence: 3,
				Request:  CacheCommandRequest{Command: "SETSTR", Key: "three", Value: "3"},
			},
		)
		if _, err := OpenCommandJournal(path); err == nil || !strings.Contains(err.Error(), "does not continue after 1") {
			t.Fatalf("OpenCommandJournal(gap) error = %v, want sequence gap error", err)
		}
		if _, err := readCommandJournalTail(path, 0, 0); err == nil || !strings.Contains(err.Error(), "does not continue after 1") {
			t.Fatalf("readCommandJournalTail(gap) error = %v, want sequence gap error", err)
		}
	})

	t.Run("missing prefix", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "commands.journal")
		writeCommandJournalTestEntries(t, path, commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 2,
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "two", Value: "2"},
		})
		if _, err := OpenCommandJournal(path); err == nil || !strings.Contains(err.Error(), "starts at sequence 2 without checkpoint") {
			t.Fatalf("OpenCommandJournal(missing prefix) error = %v, want missing prefix error", err)
		}
		if _, err := readCommandJournalTail(path, 0, 0); err == nil || !strings.Contains(err.Error(), "starts at sequence 2 without checkpoint") {
			t.Fatalf("readCommandJournalTail(missing prefix) error = %v, want missing prefix error", err)
		}
	})
}

func TestCommandJournalRejectsMalformedEntryRequests(t *testing.T) {
	t.Run("checkpoint with request", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "commands.journal")
		writeCommandJournalTestEntries(t, path, commandJournalEntry{
			Version:    commandJournalVersion,
			Sequence:   1,
			Checkpoint: true,
			Request:    CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"},
		})
		if _, err := OpenCommandJournal(path); err == nil || !strings.Contains(err.Error(), "checkpoint cannot include a request") {
			t.Fatalf("OpenCommandJournal(checkpoint request) error = %v, want checkpoint request error", err)
		}
		if _, err := readCommandJournalTail(path, 0, 0); err == nil || !strings.Contains(err.Error(), "checkpoint cannot include a request") {
			t.Fatalf("readCommandJournalTail(checkpoint request) error = %v, want checkpoint request error", err)
		}
	})

	t.Run("empty command", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "commands.journal")
		writeCommandJournalTestEntries(t, path, commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 1,
		})
		if _, err := OpenCommandJournal(path); err == nil || !strings.Contains(err.Error(), "request is not journalable") {
			t.Fatalf("OpenCommandJournal(empty request) error = %v, want non-journalable request error", err)
		}
		if _, err := readCommandJournalTail(path, 0, 0); err == nil || !strings.Contains(err.Error(), "request is not journalable") {
			t.Fatalf("readCommandJournalTail(empty request) error = %v, want non-journalable request error", err)
		}
	})

	t.Run("read-only command", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "commands.journal")
		writeCommandJournalTestEntries(t, path, commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 1,
			Request:  CacheCommandRequest{Command: "GET", Key: "name"},
		})
		if _, err := OpenCommandJournal(path); err == nil || !strings.Contains(err.Error(), "request is not journalable") {
			t.Fatalf("OpenCommandJournal(read-only request) error = %v, want non-journalable request error", err)
		}
		if _, err := readCommandJournalTail(path, 0, 0); err == nil || !strings.Contains(err.Error(), "request is not journalable") {
			t.Fatalf("readCommandJournalTail(read-only request) error = %v, want non-journalable request error", err)
		}
	})
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

func TestCommandJournalTailScansLargeHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	largeValue := strings.Repeat("x", 70*1024)
	writeCommandJournalTestEntries(t, path,
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 1,
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "large", Value: largeValue},
		},
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 2,
			Request:  CacheCommandRequest{Command: "INC", Key: "count"},
		},
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 3,
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "three", Value: "3"},
		},
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 4,
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "four", Value: "4"},
		},
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 5,
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "five", Value: "5"},
		},
	)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	if _, err := file.WriteString(`{"version":1`); err != nil {
		file.Close()
		t.Fatalf("WriteString(partial tail) error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	tail, err := readCommandJournalTail(path, 1, 2)
	if err != nil {
		t.Fatalf("readCommandJournalTail() error = %v", err)
	}
	if tail.LastSequence != 5 || tail.Limit != 2 || !tail.HasMore || len(tail.Entries) != 2 {
		t.Fatalf("large tail = %#v, want limited tail through sequence 5", tail)
	}
	if tail.Entries[0].Sequence != 2 || tail.Entries[0].Request.Key != "count" {
		t.Fatalf("first large tail entry = %#v, want sequence 2 count", tail.Entries[0])
	}
	if tail.Entries[1].Sequence != 3 || tail.Entries[1].Request.Key != "three" {
		t.Fatalf("second large tail entry = %#v, want sequence 3 three", tail.Entries[1])
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

func TestCommandJournalReplaysXorFilterMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "CREATEXF", Key: "seen", Value: "8"}); !got.OK {
		t.Fatalf("journaled CREATEXF response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ADDXF", Key: "seen", Values: Slice{"alpha", "beta"}}); !got.OK {
		t.Fatalf("journaled ADDXF response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "BUILDXF", Key: "seen"}); !got.OK {
		t.Fatalf("journaled BUILDXF response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "HASXF", Key: "seen", Value: "beta"}); !got.OK || got.Value != "1" {
		t.Fatalf("journaled HASXF response = %#v, want local read hit", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("journal entries = %d, want CREATEXF, ADDXF, and BUILDXF only", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if hit, queryable := replayed.HasXorFilter("seen", "alpha"); !queryable || !hit {
		t.Fatalf("replayed XOR filter alpha = %v/%v, want hit", hit, queryable)
	}
	if info, ok := replayed.XorFilterInfo("seen"); !ok || !info.Built || info.Items != 2 {
		t.Fatalf("replayed XorFilterInfo = %#v/%v, want built two-item filter", info, ok)
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

func TestCommandJournalReplaysRadixTreeMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "CREATERT", Key: "index"}); !got.OK {
		t.Fatalf("journaled CREATERT response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{
		Command: "PUTRT",
		Key:     "index",
		Pairs:   Map{"user:100": "active", "user:101": json.Number("42")},
	}); !got.OK {
		t.Fatalf("journaled PUTRT response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "DELRT", Key: "index", Subkey: "user:100"}); !got.OK {
		t.Fatalf("journaled DELRT response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "HASRT", Key: "index", Subkey: "user:101"}); !got.OK || got.Value != "1" {
		t.Fatalf("journaled HASRT response = %#v, want local read hit", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("journal entries = %d, want CREATERT, PUTRT, and DELRT only", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if replayed.HasRadixTree("index", "user:100") {
		t.Fatal("replayed radix tree contains removed value user:100")
	}
	if value, ok := replayed.GetRadixTree("index", "user:101"); !ok || value != json.Number("42") {
		t.Fatalf("replayed radix tree user:101 = %#v/%v, want json.Number 42", value, ok)
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

func TestCommandJournalReplaysReservoirSampleMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "CREATERS", Key: "sample", Value: "3"}); !got.OK {
		t.Fatalf("journaled CREATERS response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ADDRS", Key: "sample", Values: Slice{"alpha", "beta", "gamma", "delta"}}); !got.OK {
		t.Fatalf("journaled ADDRS response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "GETRS", Key: "sample"}); !got.OK || got.Value == "" {
		t.Fatalf("journaled GETRS response = %#v, want local sample", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("journal entries = %d, want CREATERS and ADDRS only", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetReservoirSample("sample"); len(got) != 3 {
		t.Fatalf("replayed reservoir sample len = %d, want capacity 3: %#v", len(got), got)
	}
	if info, ok := replayed.ReservoirSampleInfo("sample"); !ok || info.Seen != 4 || info.Tracked != 3 || info.Capacity != 3 {
		t.Fatalf("replayed ReservoirSampleInfo = %#v/%v, want seen 4 tracked 3", info, ok)
	}
}

func TestCommandJournalReplaysQuantileSketchMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "CREATEQ", Key: "latency", Value: "0.01"}); !got.OK {
		t.Fatalf("journaled CREATEQ response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ADDQ", Key: "latency", Values: Slice{json.Number("10"), json.Number("20"), json.Number("30")}}); !got.OK {
		t.Fatalf("journaled ADDQ response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ESTQ", Key: "latency", Value: "0.5"}); !got.OK || got.Value == "" {
		t.Fatalf("journaled ESTQ response = %#v, want local estimate", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("journal entries = %d, want CREATEQ and ADDQ only", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got, ok := replayed.EstimateQuantileSketch("latency", 0.5); !ok || got.Count != 3 || got.Value < 10 || got.Value > 30 {
		t.Fatalf("replayed quantile estimate = %#v/%v, want restored sketch", got, ok)
	}
	if info, ok := replayed.QuantileSketchInfo("latency"); !ok || info.Count != 3 || info.SummarySize == 0 {
		t.Fatalf("replayed QuantileSketchInfo = %#v/%v, want count 3", info, ok)
	}
}

func TestCommandJournalReplaysFenwickTreeMutations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}

	ht := newTestTrie(t)
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "CREATEFW", Key: "scores", Value: "8"}); !got.OK {
		t.Fatalf("journaled CREATEFW response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "ADDFW", Key: "scores", Value: "2", Subkey: "5"}); !got.OK {
		t.Fatalf("journaled ADDFW response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SUMFW", Key: "scores", Value: "2"}); !got.OK || got.Value != "5" {
		t.Fatalf("journaled SUMFW response = %#v, want local prefix sum", got)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("journal entries = %d, want CREATEFW and ADDFW only", len(entries))
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got, ok := replayed.PrefixSumFenwickTree("scores", 2); !ok || got != 5 {
		t.Fatalf("replayed Fenwick prefix sum = %d/%v, want 5", got, ok)
	}
	if info, ok := replayed.FenwickTreeInfo("scores"); !ok || info.Size != 8 || info.Updates != 1 || info.Total != 5 {
		t.Fatalf("replayed FenwickTreeInfo = %#v/%v, want one update", info, ok)
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

func TestCommandJournalRemovesRejectedRuntimeCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	ht := newTestTrie(t)

	ht.UpsertCounter("first_max", maxCommandInt32)
	if response := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "first_max", Value: "1"}); response.OK {
		t.Fatalf("ExecuteCommand(first INC overflow) = %#v, want rejection", response)
	}
	if journal.Sequence() != 0 {
		t.Fatalf("Sequence() after rejected first command = %d, want 0", journal.Sequence())
	}
	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries(first rejection) error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries after rejected first command = %#v, want none", entries)
	}
	if got := ht.GetCounter("first_max"); got != maxCommandInt32 {
		t.Fatalf("first counter after overflow = %d, want max int32", got)
	}

	if response := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETINT", Key: "max", Value: "2147483647"}); !response.OK {
		t.Fatalf("ExecuteCommand(SETINT max) = %#v, want ok", response)
	}
	if response := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "max", Value: "1"}); response.OK {
		t.Fatalf("ExecuteCommand(INC overflow) = %#v, want rejection", response)
	}
	if got := ht.GetCounter("max"); got != maxCommandInt32 {
		t.Fatalf("counter after overflow = %d, want max int32", got)
	}
	if journal.Sequence() != 1 {
		t.Fatalf("Sequence() after overflow rollback = %d, want 1", journal.Sequence())
	}
	entries, err = readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries(overflow rollback) error = %v", err)
	}
	if len(entries) != 1 || entries[0].Sequence != 1 || entries[0].Request.Command != "SETINT" {
		t.Fatalf("entries after overflow rollback = %#v, want only SETINT sequence 1", entries)
	}

	if response := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !response.OK {
		t.Fatalf("ExecuteCommand(after rollback) = %#v, want ok", response)
	}
	entries, err = readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries(after valid command) error = %v", err)
	}
	if len(entries) != 2 || entries[1].Sequence != 2 || entries[1].Request.Command != "SETSTR" {
		t.Fatalf("entries after valid command = %#v, want SETSTR sequence 2", entries)
	}
}

func TestCommandJournalRollbackFailedAppendRemovesPartialBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	ht := newTestTrie(t)

	if response := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "first", Value: "ok"}); !response.OK {
		t.Fatalf("ExecuteCommand(first) = %#v, want ok", response)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(before) error = %v", err)
	}
	info, err := journal.file.Stat()
	if err != nil {
		t.Fatalf("journal Stat() error = %v", err)
	}
	state := commandJournalAppendState{
		offset:            info.Size(),
		nextSequence:      journal.nextSequence,
		sequenceExhausted: journal.sequenceExhausted,
	}

	if _, err := journal.file.WriteString(`{"version":1,"sequence":2,`); err != nil {
		t.Fatalf("WriteString(partial) error = %v", err)
	}
	journal.nextSequence = 99
	errBoom := errors.New("boom")
	if err := journal.rollbackFailedAppendLocked(state, errBoom); !errors.Is(err, errBoom) {
		t.Fatalf("rollbackFailedAppendLocked() error = %v, want boom", err)
	}
	if journal.nextSequence != state.nextSequence || journal.sequenceExhausted != state.sequenceExhausted {
		t.Fatalf("journal sequence after rollback = %d/%v, want %d/%v", journal.nextSequence, journal.sequenceExhausted, state.nextSequence, state.sequenceExhausted)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(after rollback) error = %v", err)
	}
	if string(after) != string(before) {
		t.Fatalf("journal after rollback = %q, want original %q", after, before)
	}

	if response := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "second", Value: "ok"}); !response.OK {
		t.Fatalf("ExecuteCommand(second) = %#v, want ok", response)
	}
	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 || entries[0].Sequence != 1 || entries[1].Sequence != 2 || entries[1].Request.Key != "second" {
		t.Fatalf("entries after rollback and append = %#v, want contiguous first/second commands", entries)
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

func TestCommandJournalSnapshotAllowsWritesBetweenPagesAndCapturesFinalSequence(t *testing.T) {
	dir := t.TempDir()
	journalPath := filepath.Join(dir, "commands.journal")
	snapshotPath := filepath.Join(dir, "snapshot.hc")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	ht := newTestTrie(t)
	for idx := 0; idx < snapshotCaptureScanPageEntries*2; idx++ {
		ht.UpsertString(fmt.Sprintf("key:%05d", idx), "before")
	}

	firstPage := make(chan struct{})
	release := make(chan struct{})
	ht.snapshotCapturePageHook = func(page int) {
		if page == 1 {
			close(firstPage)
			<-release
		}
	}
	saved := make(chan error, 1)
	go func() {
		saved <- journal.SaveSnapshot(ht, snapshotPath)
	}()
	<-firstPage

	written := make(chan CacheCommandResponse, 1)
	go func() {
		written <- journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "key:00000", Value: "after"})
	}()
	select {
	case response := <-written:
		if !response.OK {
			t.Fatalf("ExecuteCommand() = %#v, want success", response)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("journaled writer could not complete between snapshot pages")
	}
	close(release)
	if err := <-saved; err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := newTestTrie(t)
	metadata, err := loaded.LoadSnapshotWithMetadata(snapshotPath)
	if err != nil {
		t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
	}
	if metadata.JournalSequence != 1 {
		t.Fatalf("snapshot journal sequence = %d, want concurrent write sequence 1", metadata.JournalSequence)
	}
	if got := loaded.GetString("key:00000"); got != "after" {
		t.Fatalf("snapshot concurrent value = %q, want after", got)
	}
}

func TestCommandJournalStreamsAndReplacesFromExactSnapshot(t *testing.T) {
	sourceTrie := newTestTrie(t)
	sourceJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "source.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal(source) error = %v", err)
	}
	defer sourceJournal.Close()
	for _, request := range []CacheCommandRequest{
		{Command: "SETSTR", Key: "kept", Value: "source"},
		{Command: "SETSTR", Key: "second", Value: "value"},
	} {
		if response := sourceJournal.ExecuteCommand(sourceTrie, request); !response.OK {
			t.Fatalf("source command response = %#v, want ok", response)
		}
	}

	var snapshot bytes.Buffer
	metadata, err := sourceJournal.WriteSnapshotWithFormat(sourceTrie, &snapshot, SnapshotFormatGzipBinary)
	if err != nil {
		t.Fatalf("WriteSnapshotWithFormat() error = %v", err)
	}
	if metadata.JournalSequence != 2 {
		t.Fatalf("snapshot sequence = %d, want 2", metadata.JournalSequence)
	}
	if tail, err := sourceJournal.Tail(0, 10); err != nil || len(tail.Entries) != 2 {
		t.Fatalf("source tail after stream = %#v/%v, want two uncompacted entries", tail, err)
	}

	snapshotPath := filepath.Join(t.TempDir(), "source.hc")
	if err := os.WriteFile(snapshotPath, snapshot.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile(snapshot) error = %v", err)
	}
	targetTrie := newTestTrie(t)
	targetTrie.UpsertString("stale", "remove")
	targetJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "target.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal(target) error = %v", err)
	}
	defer targetJournal.Close()

	restored, err := targetJournal.ReplaceWithSnapshot(targetTrie, snapshotPath)
	if err != nil {
		t.Fatalf("ReplaceWithSnapshot() error = %v", err)
	}
	if restored.JournalSequence != 2 || targetTrie.GetString("kept") != "source" || targetTrie.Exists("stale") {
		t.Fatalf("restored sequence/kept/stale = %d/%q/%v, want 2/source/false", restored.JournalSequence, targetTrie.GetString("kept"), targetTrie.Exists("stale"))
	}
	tail, err := targetJournal.Tail(0, 10)
	if !errors.Is(err, ErrCommandJournalCompacted) || tail.CompactedThrough != 2 {
		t.Fatalf("target tail after replacement = %#v/%v, want checkpoint 2", tail, err)
	}
}

func TestCommandJournalSnapshotOutputDoesNotBlockLaterCommands(t *testing.T) {
	trie := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "key", Value: "before"}); !response.OK {
		t.Fatalf("initial command response = %#v, want ok", response)
	}

	writer := &blockingSnapshotWriter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	type snapshotResult struct {
		metadata SnapshotMetadata
		err      error
	}
	written := make(chan snapshotResult, 1)
	go func() {
		metadata, err := journal.WriteSnapshotWithFormat(trie, writer, SnapshotFormatBinary)
		written <- snapshotResult{metadata: metadata, err: err}
	}()
	<-writer.entered

	commandDone := make(chan CacheCommandResponse, 1)
	go func() {
		commandDone <- journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "key", Value: "after"})
	}()
	commandCompletedDuringOutput := false
	var response CacheCommandResponse
	select {
	case response = <-commandDone:
		commandCompletedDuringOutput = true
	case <-time.After(500 * time.Millisecond):
	}
	close(writer.release)
	result := <-written
	if result.err != nil {
		t.Fatalf("WriteSnapshotWithFormat() error = %v", result.err)
	}
	if !commandCompletedDuringOutput {
		response = <-commandDone
		t.Fatal("snapshot output held the journal lock while the writer was blocked")
	}
	if !response.OK {
		t.Fatalf("later command response = %#v, want ok", response)
	}
	if result.metadata.JournalSequence != 1 {
		t.Fatalf("snapshot sequence = %d, want 1", result.metadata.JournalSequence)
	}

	snapshotPath := filepath.Join(t.TempDir(), "captured.hc")
	if err := os.WriteFile(snapshotPath, writer.buffer.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile(snapshot) error = %v", err)
	}
	restored := newTestTrie(t)
	metadata, err := restored.LoadSnapshotWithMetadata(snapshotPath)
	if err != nil {
		t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
	}
	if metadata.JournalSequence != 1 || restored.GetString("key") != "before" {
		t.Fatalf("restored sequence/value = %d/%q, want 1/before", metadata.JournalSequence, restored.GetString("key"))
	}
	tail, err := journal.Tail(1, 10)
	if err != nil {
		t.Fatalf("Tail(after snapshot) error = %v", err)
	}
	if len(tail.Entries) != 1 || tail.Entries[0].Sequence != 2 {
		t.Fatalf("tail after snapshot = %#v, want sequence 2", tail)
	}
}

func TestCommandJournalCompactionStreamsLargeHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	largeValue := strings.Repeat("x", 70*1024)
	writeCommandJournalTestEntries(t, path,
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 1,
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "old-large", Value: largeValue},
		},
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 2,
			Request:  CacheCommandRequest{Command: "INC", Key: "views"},
		},
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 3,
			Request:  CacheCommandRequest{Command: "INC", Key: "views"},
		},
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 4,
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "kept-large", Value: largeValue},
		},
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: 5,
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "kept-small", Value: "ok"},
		},
	)

	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	journal.mu.Lock()
	err = journal.compactLocked(3)
	journal.mu.Unlock()
	if err != nil {
		t.Fatalf("compactLocked() error = %v", err)
	}

	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries after compact = %#v, want checkpoint plus two retained entries", entries)
	}
	if !entries[0].Checkpoint || entries[0].Sequence != 3 {
		t.Fatalf("compaction checkpoint = %#v, want sequence 3 checkpoint", entries[0])
	}
	if entries[1].Sequence != 4 || entries[1].Request.Key != "kept-large" || entries[1].Request.Value != largeValue {
		t.Fatalf("first retained entry = %#v, want sequence 4 kept-large", entries[1])
	}
	if entries[2].Sequence != 5 || entries[2].Request.Key != "kept-small" {
		t.Fatalf("second retained entry = %#v, want sequence 5 kept-small", entries[2])
	}

	ht := newTestTrie(t)
	if response := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "after", Value: "append"}); !response.OK {
		t.Fatalf("ExecuteCommand(after compact) = %#v, want ok", response)
	}
	entries, err = readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries(after append) error = %v", err)
	}
	if last := entries[len(entries)-1]; last.Sequence != 6 || last.Request.Key != "after" {
		t.Fatalf("entry after compact append = %#v, want sequence 6 after", last)
	}
}

func TestCommandJournalReplayReportsCompactedBoundary(t *testing.T) {
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

	replayJournal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	replayed := newTestTrie(t)
	maxSequence, err := replayJournal.Replay(replayed, 0)
	if !errors.Is(err, ErrCommandJournalCompacted) {
		t.Fatalf("Replay(before compacted boundary) error = %v, want ErrCommandJournalCompacted", err)
	}
	if maxSequence != 0 {
		t.Fatalf("Replay(before compacted boundary) max sequence = %d, want 0", maxSequence)
	}
	if got := replayed.GetCounter("views"); got != 0 {
		t.Fatalf("replayed views before compacted boundary = %d, want empty trie", got)
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

func TestCommandJournalRejectsNilReceiverAndTrie(t *testing.T) {
	ht := newTestTrie(t)
	var nilJournal *CommandJournal
	response := nilJournal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"})
	if response.OK || response.Message != ErrNilCommandJournal.Error() {
		t.Fatalf("ExecuteCommand(nil journal) = %#v, want nil journal error", response)
	}
	if ht.Exists("name") {
		t.Fatal("ExecuteCommand(nil journal) mutated trie")
	}
	if got := nilJournal.Sequence(); got != 0 {
		t.Fatalf("Sequence(nil journal) = %d, want 0", got)
	}
	if _, err := nilJournal.Replay(ht, 0); !errors.Is(err, ErrNilCommandJournal) {
		t.Fatalf("Replay(nil journal) error = %v, want ErrNilCommandJournal", err)
	}
	if _, err := nilJournal.Tail(0, 0); !errors.Is(err, ErrNilCommandJournal) {
		t.Fatalf("Tail(nil journal) error = %v, want ErrNilCommandJournal", err)
	}
	nilSnapshotPath := filepath.Join(t.TempDir(), "nil-journal-snapshot.json")
	if err := nilJournal.SaveSnapshot(ht, nilSnapshotPath); !errors.Is(err, ErrNilCommandJournal) {
		t.Fatalf("SaveSnapshot(nil journal) error = %v, want ErrNilCommandJournal", err)
	}
	if _, err := os.Stat(nilSnapshotPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SaveSnapshot(nil journal) created path/stat error = %v, want not exist", err)
	}

	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	response = journal.ExecuteCommand(nil, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"})
	if response.OK || response.Message != ErrNilHatTrie.Error() {
		t.Fatalf("ExecuteCommand(nil trie) = %#v, want nil trie error", response)
	}
	if _, err := journal.Replay(nil, 0); !errors.Is(err, ErrNilHatTrie) {
		t.Fatalf("Replay(nil trie) error = %v, want ErrNilHatTrie", err)
	}
	nilTrieSnapshotPath := filepath.Join(t.TempDir(), "nil-trie-snapshot.json")
	if err := journal.SaveSnapshot(nil, nilTrieSnapshotPath); !errors.Is(err, ErrNilHatTrie) {
		t.Fatalf("SaveSnapshot(nil trie) error = %v, want ErrNilHatTrie", err)
	}
	if _, err := os.Stat(nilTrieSnapshotPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SaveSnapshot(nil trie) created path/stat error = %v, want not exist", err)
	}
}

func TestCommandJournalRejectsAppendAfterSequenceExhaustionWithoutMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	writeCommandJournalTestEntries(t, path, commandJournalEntry{
		Version:    commandJournalVersion,
		Sequence:   ^uint64(0) - 1,
		Checkpoint: true,
	})
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	ht := newTestTrie(t)

	if response := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "last", Value: "ok"}); !response.OK {
		t.Fatalf("ExecuteCommand(final sequence) = %#v, want ok", response)
	}
	if got := journal.Sequence(); got != ^uint64(0) {
		t.Fatalf("Sequence() after final append = %d, want max uint64", got)
	}
	if got := ht.GetString("last"); got != "ok" {
		t.Fatalf("final journaled command stored %q, want ok", got)
	}
	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 || entries[0].Sequence != ^uint64(0)-1 || !entries[0].Checkpoint || entries[1].Sequence != ^uint64(0) {
		t.Fatalf("entries after final append = %#v, want checkpoint and max sequence entry", entries)
	}

	response := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "overflow", Value: "bad"})
	if response.OK || response.Message != ErrCommandJournalSequenceExhausted.Error() {
		t.Fatalf("ExecuteCommand(exhausted) = %#v, want sequence exhausted error", response)
	}
	if ht.Exists("overflow") {
		t.Fatal("ExecuteCommand(exhausted) mutated trie")
	}
	entries, err = readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries(after exhausted) error = %v", err)
	}
	if len(entries) != 2 || entries[0].Sequence != ^uint64(0)-1 || !entries[0].Checkpoint || entries[1].Sequence != ^uint64(0) {
		t.Fatalf("entries after exhausted append = %#v, want checkpoint and original max sequence entry only", entries)
	}
}

func TestOpenCommandJournalAtMaxSequenceReplaysButRejectsAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	writeCommandJournalTestEntries(t, path,
		commandJournalEntry{
			Version:    commandJournalVersion,
			Sequence:   ^uint64(0) - 1,
			Checkpoint: true,
		},
		commandJournalEntry{
			Version:  commandJournalVersion,
			Sequence: ^uint64(0),
			Request:  CacheCommandRequest{Command: "SETSTR", Key: "last", Value: "ok"},
		},
	)

	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(max sequence) error = %v", err)
	}
	if got := journal.Sequence(); got != ^uint64(0) {
		t.Fatalf("Sequence() after open = %d, want max uint64", got)
	}
	tail, err := journal.Tail(^uint64(0)-1, 0)
	if err != nil {
		t.Fatalf("Tail(max-1) error = %v", err)
	}
	if tail.LastSequence != ^uint64(0) || len(tail.Entries) != 1 || tail.Entries[0].Sequence != ^uint64(0) {
		t.Fatalf("Tail(max-1) = %#v, want max sequence entry", tail)
	}

	replayed := newTestTrie(t)
	if maxSequence, err := journal.Replay(replayed, ^uint64(0)-1); err != nil || maxSequence != ^uint64(0) {
		t.Fatalf("Replay(max sequence) = %d/%v, want max/nil", maxSequence, err)
	}
	if got := replayed.GetString("last"); got != "ok" {
		t.Fatalf("replayed last = %q, want ok", got)
	}
	response := journal.ExecuteCommand(replayed, CacheCommandRequest{Command: "SETSTR", Key: "overflow", Value: "bad"})
	if response.OK || response.Message != ErrCommandJournalSequenceExhausted.Error() {
		t.Fatalf("ExecuteCommand(after max open) = %#v, want sequence exhausted error", response)
	}
	if replayed.Exists("overflow") {
		t.Fatal("ExecuteCommand(after max open) mutated trie")
	}
	entries, err := readCommandJournalEntries(path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 || entries[0].Sequence != ^uint64(0)-1 || !entries[0].Checkpoint || entries[1].Sequence != ^uint64(0) {
		t.Fatalf("entries after rejected append = %#v, want checkpoint and original max sequence entry only", entries)
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
