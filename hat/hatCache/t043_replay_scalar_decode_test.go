package hatCache

import (
	"path/filepath"
	"strconv"
	"testing"
)

func TestT043DecodeBinaryScalarReplayRecord(t *testing.T) {
	tests := []struct {
		name       string
		request    CacheCommandRequest
		wantOK     bool
		wantFamily nativeCommandBatchFamily
	}{
		{
			name:       "set",
			request:    CacheCommandRequest{Command: "SET", Key: "key", Value: "value"},
			wantOK:     true,
			wantFamily: nativeCommandBatchSetString,
		},
		{
			name:       "setint",
			request:    CacheCommandRequest{Command: "SETINT", Key: "counter", Value: "7"},
			wantOK:     true,
			wantFamily: nativeCommandBatchSetCounter,
		},
		{
			name:    "unsupported command",
			request: CacheCommandRequest{Command: "DEL", Key: "key"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := t043ScalarPayload(t, commandJournalEntry{
				Version:  commandJournalVersion,
				Sequence: 17,
				Request:  test.request,
			})
			entry, family, ok, err := decodeCommandJournalScalarRecordBinaryPayload(payload)
			if err != nil {
				t.Fatalf("decodeCommandJournalScalarRecordBinaryPayload() error = %v", err)
			}
			if ok != test.wantOK {
				t.Fatalf("decodeCommandJournalScalarRecordBinaryPayload() ok = %v, want %v", ok, test.wantOK)
			}
			if family != test.wantFamily {
				t.Fatalf("decodeCommandJournalScalarRecordBinaryPayload() family = %d, want %d", family, test.wantFamily)
			}
			if ok {
				if entry.Sequence != 17 || entry.Request.Key != test.request.Key || entry.Request.Value != test.request.Value {
					t.Fatalf("decoded entry = %#v, want sequence/key/value 17/%q/%q", entry, test.request.Key, test.request.Value)
				}
			}
		})
	}
}

func TestT043DecodeBinaryScalarReplayRecordRejectsTruncatedPayload(t *testing.T) {
	payload := t043ScalarPayload(t, commandJournalEntry{
		Version:  commandJournalVersion,
		Sequence: 1,
		Request:  CacheCommandRequest{Command: "SET", Key: "key", Value: "value"},
	})
	if _, _, _, err := decodeCommandJournalScalarRecordBinaryPayload(payload[:len(payload)-1]); err == nil {
		t.Fatal("decodeCommandJournalScalarRecordBinaryPayload(truncated) error = nil, want error")
	}
}

func TestT043ReplayPreservesValuesAcrossScalarBatchFlushes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	writerTrie := CreateHatTrie()
	defer writerTrie.Destroy()
	writer, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(writer) error = %v", err)
	}
	for index := 0; index < maxCommandJournalReplayScalarBatchRecords+44; index++ {
		request := CacheCommandRequest{
			Command: "SET",
			Key:     "t043:" + strconv.Itoa(index),
			Value:   "value:" + strconv.Itoa(index),
		}
		if response := writer.ExecuteCommand(writerTrie, request); !response.OK {
			t.Fatalf("ExecuteCommand(%d) response = %#v", index, response)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}

	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	defer replayJournal.Close()
	replayed := CreateHatTrie()
	defer replayed.Destroy()
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	for index := 0; index < maxCommandJournalReplayScalarBatchRecords+44; index++ {
		key := "t043:" + strconv.Itoa(index)
		if got := replayed.GetString(key); got != "value:"+strconv.Itoa(index) {
			t.Fatalf("GetString(%q) = %q, want value", key, got)
		}
	}
}

func TestT043ReplayKeepsJSONFallbackCorrect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	writerTrie := CreateHatTrie()
	defer writerTrie.Destroy()
	writer, err := OpenCommandJournalWithFormat(path, CommandJournalFormatJSON)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat(writer) error = %v", err)
	}
	for index := 0; index < minNativeCommandBatchSize+3; index++ {
		request := CacheCommandRequest{
			Command: "SET",
			Key:     "json:" + strconv.Itoa(index),
			Value:   "value:" + strconv.Itoa(index),
		}
		if response := writer.ExecuteCommand(writerTrie, request); !response.OK {
			t.Fatalf("ExecuteCommand(%d) response = %#v", index, response)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}

	replayJournal, err := OpenCommandJournalWithFormat(path, CommandJournalFormatJSON)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat(replay) error = %v", err)
	}
	defer replayJournal.Close()
	replayed := CreateHatTrie()
	defer replayed.Destroy()
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	for index := 0; index < minNativeCommandBatchSize+3; index++ {
		key := "json:" + strconv.Itoa(index)
		if got := replayed.GetString(key); got != "value:"+strconv.Itoa(index) {
			t.Fatalf("GetString(%q) = %q, want value", key, got)
		}
	}
}

func TestT043ReplayFallsBackForTTLCommands(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	writerTrie := CreateHatTrie()
	defer writerTrie.Destroy()
	writer, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(writer) error = %v", err)
	}
	for index := 0; index < minNativeCommandBatchSize; index++ {
		request := CacheCommandRequest{
			Command: "SET",
			Key:     "ttl-boundary:" + strconv.Itoa(index),
			Value:   "value:" + strconv.Itoa(index),
		}
		if response := writer.ExecuteCommand(writerTrie, request); !response.OK {
			t.Fatalf("ExecuteCommand(%d) response = %#v", index, response)
		}
	}
	ttl := int64(60)
	if response := writer.ExecuteCommand(writerTrie, CacheCommandRequest{Command: "SET", Key: "ttl-boundary-value", Value: "ttl-value", TTLSeconds: &ttl}); !response.OK {
		t.Fatalf("ExecuteCommand(SET with ttl) response = %#v", response)
	}
	if response := writer.ExecuteCommand(writerTrie, CacheCommandRequest{Command: "SETINT", Key: "ttl-boundary-counter", Value: "7", TTLSeconds: &ttl}); !response.OK {
		t.Fatalf("ExecuteCommand(SETINT with ttl) response = %#v", response)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}

	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	defer replayJournal.Close()
	replayed := CreateHatTrie()
	defer replayed.Destroy()
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetString("ttl-boundary-value"); got != "ttl-value" {
		t.Fatalf("GetString(ttl-boundary-value) = %q, want ttl-value", got)
	}
	if got := replayed.GetCounter("ttl-boundary-counter"); got != 7 {
		t.Fatalf("GetCounter(ttl-boundary-counter) = %d, want 7", got)
	}
}

func t043ScalarPayload(t *testing.T, entry commandJournalEntry) []byte {
	t.Helper()
	raw, err := marshalCommandJournalEntryBinary(entry)
	if err != nil {
		t.Fatalf("marshalCommandJournalEntryBinary() error = %v", err)
	}
	reader := newBinaryFieldReader(raw[len(commandJournalBinaryMagic):])
	payload, err := reader.readBytes()
	if err != nil {
		t.Fatalf("read binary journal payload error = %v", err)
	}
	if !reader.done() {
		t.Fatal("binary journal payload has trailing bytes")
	}
	return payload
}
