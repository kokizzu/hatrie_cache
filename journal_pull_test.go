package hatriecache

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCommandJournalPullErrorWrapsUnderlyingError(t *testing.T) {
	sentinel := errors.New("source unavailable")
	err := commandJournalPullError(http.StatusBadGateway, sentinel)

	var pullErr *CommandJournalPullError
	if !errors.As(err, &pullErr) {
		t.Fatalf("error = %T, want CommandJournalPullError", err)
	}
	if pullErr.Status != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", pullErr.Status, http.StatusBadGateway)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is(%v, sentinel) = false, want true", err)
	}
	if got := err.Error(); got != sentinel.Error() {
		t.Fatalf("Error() = %q, want wrapped message", got)
	}
}

func TestPullCommandJournalValidationErrorsCarryHTTPStatus(t *testing.T) {
	_, err := PullCommandJournal(context.Background(), nil, nil, CommandJournalPullOptions{})
	var pullErr *CommandJournalPullError
	if !errors.As(err, &pullErr) || pullErr.Status != http.StatusBadRequest || !strings.Contains(err.Error(), "trie is not configured") {
		t.Fatalf("PullCommandJournal(nil trie) error = %v/%#v, want 400 pull error", err, pullErr)
	}

	ht := newTestTrie(t)
	_, err = PullCommandJournal(context.Background(), ht, nil, CommandJournalPullOptions{})
	pullErr = nil
	if !errors.As(err, &pullErr) || pullErr.Status != http.StatusConflict || !strings.Contains(err.Error(), "journal is not configured") {
		t.Fatalf("PullCommandJournal(nil journal) error = %v/%#v, want 409 pull error", err, pullErr)
	}
}

func TestCommandJournalPullHTTPClientTimeouts(t *testing.T) {
	client, err := commandJournalPullHTTPClient(nil, 250*time.Millisecond)
	if err != nil {
		t.Fatalf("commandJournalPullHTTPClient(timeout) error = %v", err)
	}
	if client == nil || client.Timeout != 250*time.Millisecond {
		t.Fatalf("client timeout = %v, want 250ms", client)
	}

	client, err = commandJournalPullHTTPClient(nil, 0)
	if err != nil {
		t.Fatalf("commandJournalPullHTTPClient(no timeout) error = %v", err)
	}
	if client != http.DefaultClient {
		t.Fatalf("client = %p, want http.DefaultClient %p", client, http.DefaultClient)
	}

	custom := &http.Client{Timeout: time.Second}
	client, err = commandJournalPullHTTPClient(custom, 250*time.Millisecond)
	if err != nil {
		t.Fatalf("commandJournalPullHTTPClient(custom) error = %v", err)
	}
	if client != custom || client.Timeout != time.Second {
		t.Fatalf("custom client = %#v, want original custom timeout", client)
	}

	if _, err := commandJournalPullHTTPClient(nil, -time.Second); err == nil {
		t.Fatal("commandJournalPullHTTPClient(negative) error = nil, want rejection")
	}
}

func TestPullCommandJournalSnapshotKeepsPreviousFileWhenDownloadIsInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pull.hc")
	if err := os.WriteFile(path, []byte("previous-snapshot"), 0o600); err != nil {
		t.Fatalf("WriteFile(previous) error = %v", err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", snapshotContentType)
		_, _ = io.WriteString(w, "not-a-snapshot")
	}))
	defer source.Close()

	if _, err := PullCommandJournalSnapshot(context.Background(), source.URL, "", source.Client(), path); err == nil {
		t.Fatal("PullCommandJournalSnapshot() error = nil, want invalid snapshot rejection")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(previous) error = %v", err)
	}
	if got := string(data); got != "previous-snapshot" {
		t.Fatalf("snapshot after invalid download = %q, want previous file", got)
	}
}

func TestPullCommandJournalSnapshotKeepsPreviousFileWhenSequenceRegresses(t *testing.T) {
	sourceTrie := newTestTrie(t)
	sourceJournal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "source.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal(source) error = %v", err)
	}
	defer sourceJournal.Close()
	if response := sourceJournal.ExecuteCommand(sourceTrie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "old"}); !response.OK {
		t.Fatalf("source SETSTR response = %#v, want ok", response)
	}
	var snapshot strings.Builder
	if _, err := sourceJournal.WriteSnapshotWithFormat(sourceTrie, &snapshot, SnapshotFormatJSON); err != nil {
		t.Fatalf("WriteSnapshotWithFormat() error = %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", snapshotContentType)
		_, _ = io.WriteString(w, snapshot.String())
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "pull.hc")
	if err := os.WriteFile(path, []byte("previous-snapshot"), 0o600); err != nil {
		t.Fatalf("WriteFile(previous) error = %v", err)
	}

	if _, err := PullCommandJournalSnapshot(context.Background(), server.URL, "", server.Client(), path, 2); err == nil {
		t.Fatal("PullCommandJournalSnapshot() error = nil, want sequence regression rejection")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(previous) error = %v", err)
	}
	if got := string(data); got != "previous-snapshot" {
		t.Fatalf("snapshot after sequence regression = %q, want previous file", got)
	}
}

func TestPullCommandJournalAcceptsNilContext(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := CommandJournalTail{
			LastSequence: 1,
			Entries: []CommandJournalRecord{
				{Sequence: 1, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}))
	defer source.Close()

	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	dirty := NewLevelDBDirtyTracker()

	result, err := PullCommandJournal(nil, ht, journal, CommandJournalPullOptions{Source: source.URL, DirtyTracker: dirty})
	if err != nil {
		t.Fatalf("PullCommandJournal(nil context) error = %v", err)
	}
	if result.Applied != 1 || result.AppliedThrough != 1 || result.LastSequence != 1 {
		t.Fatalf("pull result = %#v, want one applied entry through sequence 1", result)
	}
	if got := ht.GetString("name"); got != "ivi" {
		t.Fatalf("GetString(name) = %q, want replicated value", got)
	}
	if dirty.Pending() != 1 {
		t.Fatalf("dirty pending after pull = %d, want pulled key marked", dirty.Pending())
	}
}

func TestApplyCommandJournalTailPersistsOnlySuccessfulPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	result, err := applyCommandJournalTail(ht, journal, "source", 0, CommandJournalTail{
		LastSequence: 2,
		Entries: []CommandJournalRecord{
			{Sequence: 1, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
			{Sequence: 2, Request: CacheCommandRequest{Command: "INC", Key: "views", Value: "invalid"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "journal entry 2 failed") {
		t.Fatalf("applyCommandJournalTail() error = %v, want entry 2 failure", err)
	}
	if result.Applied != 1 || result.AppliedThrough != 1 || ht.GetString("name") != "ivi" {
		t.Fatalf("partial result/name = %#v/%q, want one applied entry", result, ht.GetString("name"))
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	defer replayJournal.Close()
	if sequence, err := replayJournal.Replay(replayed, 0); err != nil || sequence != 1 {
		t.Fatalf("Replay() sequence/error = %d/%v, want 1/nil", sequence, err)
	}
	if replayed.GetString("name") != "ivi" || replayed.Exists("views") {
		t.Fatalf("replayed name/views = %q/%v, want ivi/false", replayed.GetString("name"), replayed.Exists("views"))
	}
}

func TestApplyCommandJournalTailRejectsSequenceGap(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	result, err := applyCommandJournalTail(ht, journal, "source", 1, CommandJournalTail{
		LastSequence: 3,
		Entries: []CommandJournalRecord{
			{Sequence: 3, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "does not continue after 1") {
		t.Fatalf("applyCommandJournalTail(gap) error = %v, want sequence gap error", err)
	}
	if result.Applied != 0 || result.AppliedThrough != 1 {
		t.Fatalf("gap result = %#v, want no progress after sequence 1", result)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("gap applied value = %q, want empty", got)
	}
}

func TestApplyCommandJournalTailRejectsInternalSequenceGapWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	result, err := applyCommandJournalTail(ht, journal, "source", 0, CommandJournalTail{
		LastSequence: 3,
		Entries: []CommandJournalRecord{
			{Sequence: 1, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
			{Sequence: 3, Request: CacheCommandRequest{Command: "SETSTR", Key: "city", Value: "paris"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "does not continue after 1") {
		t.Fatalf("applyCommandJournalTail(internal gap) error = %v, want sequence gap error", err)
	}
	if result.Applied != 0 || result.AppliedThrough != 0 {
		t.Fatalf("internal gap result = %#v, want no progress after sequence 0", result)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("internal gap applied first value = %q, want empty", got)
	}
	if got := ht.GetString("city"); got != "" {
		t.Fatalf("internal gap applied second value = %q, want empty", got)
	}
	if entries, err := readCommandJournalEntries(journal.path); err != nil || len(entries) != 0 {
		t.Fatalf("journal entries after internal gap = %#v/%v, want none", entries, err)
	}
}

func TestApplyCommandJournalTailRejectsEntryPastLastSequenceWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	result, err := applyCommandJournalTail(ht, journal, "source", 0, CommandJournalTail{
		LastSequence: 0,
		Entries: []CommandJournalRecord{
			{Sequence: 1, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds last sequence 0") {
		t.Fatalf("applyCommandJournalTail(past last) error = %v, want last sequence error", err)
	}
	if result.Applied != 0 || result.AppliedThrough != 0 || result.LastSequence != 0 {
		t.Fatalf("past-last result = %#v, want no progress with last sequence 0", result)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("past-last tail applied value = %q, want empty", got)
	}
	if entries, err := readCommandJournalEntries(journal.path); err != nil || len(entries) != 0 {
		t.Fatalf("journal entries after past-last tail = %#v/%v, want none", entries, err)
	}
}

func TestApplyCommandJournalTailRejectsIncompleteTailWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	result, err := applyCommandJournalTail(ht, journal, "source", 0, CommandJournalTail{
		LastSequence: 2,
		Entries: []CommandJournalRecord{
			{Sequence: 1, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "without has_more") {
		t.Fatalf("applyCommandJournalTail(incomplete) error = %v, want missing has_more error", err)
	}
	if result.Applied != 0 || result.AppliedThrough != 0 || result.LastSequence != 2 {
		t.Fatalf("incomplete result = %#v, want no progress with last sequence 2", result)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("incomplete tail applied value = %q, want empty", got)
	}
	if entries, err := readCommandJournalEntries(journal.path); err != nil || len(entries) != 0 {
		t.Fatalf("journal entries after incomplete tail = %#v/%v, want none", entries, err)
	}
}

func TestApplyCommandJournalTailRejectsImpossibleHasMoreWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	result, err := applyCommandJournalTail(ht, journal, "source", 0, CommandJournalTail{
		LastSequence: 1,
		HasMore:      true,
		Entries: []CommandJournalRecord{
			{Sequence: 1, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "reported more entries after 1") {
		t.Fatalf("applyCommandJournalTail(impossible has_more) error = %v, want has_more last sequence error", err)
	}
	if result.Applied != 0 || result.AppliedThrough != 0 || result.LastSequence != 1 || !result.HasMore {
		t.Fatalf("impossible has_more result = %#v, want no progress with has_more preserved", result)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("impossible has_more tail applied value = %q, want empty", got)
	}
	if entries, err := readCommandJournalEntries(journal.path); err != nil || len(entries) != 0 {
		t.Fatalf("journal entries after impossible has_more tail = %#v/%v, want none", entries, err)
	}
}

func TestApplyCommandJournalTailRejectsSequenceExhaustionWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	result, err := applyCommandJournalTail(ht, journal, "source", ^uint64(0), CommandJournalTail{
		LastSequence: ^uint64(0),
		Entries: []CommandJournalRecord{
			{Sequence: 0, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
		},
	})
	if !errors.Is(err, ErrCommandJournalSequenceExhausted) {
		t.Fatalf("applyCommandJournalTail(exhausted) error = %v, want ErrCommandJournalSequenceExhausted", err)
	}
	if result.Applied != 0 || result.AppliedThrough != ^uint64(0) {
		t.Fatalf("exhausted result = %#v, want no progress after max sequence", result)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("exhausted tail applied value = %q, want empty", got)
	}
	if entries, err := readCommandJournalEntries(journal.path); err != nil || len(entries) != 0 {
		t.Fatalf("journal entries after exhausted tail = %#v/%v, want none", entries, err)
	}
}

func TestApplyCommandJournalTailRejectsCompactedGap(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	result, err := applyCommandJournalTail(ht, journal, "source", 1, CommandJournalTail{
		LastSequence:     3,
		CompactedThrough: 2,
		Entries: []CommandJournalRecord{
			{Sequence: 3, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
		},
	})
	if !errors.Is(err, ErrCommandJournalCompacted) {
		t.Fatalf("applyCommandJournalTail(compacted) error = %v, want ErrCommandJournalCompacted", err)
	}
	if result.Applied != 0 || result.AppliedThrough != 1 || result.CompactedThrough != 2 {
		t.Fatalf("compacted result = %#v, want no progress with compacted boundary", result)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("compacted gap applied value = %q, want empty", got)
	}
}

func TestPullCommandJournalReportsSequenceExhaustionAsBadGateway(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := CommandJournalTail{
			LastSequence: ^uint64(0),
			Entries: []CommandJournalRecord{
				{Sequence: 0, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}))
	defer source.Close()

	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	result, err := PullCommandJournal(context.Background(), ht, journal, CommandJournalPullOptions{
		Source:        source.URL,
		AfterSequence: ^uint64(0),
		Limit:         10,
	})
	var pullErr *CommandJournalPullError
	if !errors.As(err, &pullErr) || pullErr.Status != http.StatusBadGateway || !errors.Is(err, ErrCommandJournalSequenceExhausted) {
		t.Fatalf("PullCommandJournal(exhausted tail) error = %v/%#v, want 502 sequence exhausted", err, pullErr)
	}
	if result.Applied != 0 || result.AppliedThrough != ^uint64(0) || result.LastSequence != ^uint64(0) {
		t.Fatalf("exhausted pull result = %#v, want no progress at max sequence", result)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("exhausted pull applied value = %q, want empty", got)
	}
}

func TestPullCommandJournalReportsEntryPastLastSequenceAsBadGateway(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := CommandJournalTail{
			LastSequence: 0,
			Entries: []CommandJournalRecord{
				{Sequence: 1, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}))
	defer source.Close()

	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	result, err := PullCommandJournal(context.Background(), ht, journal, CommandJournalPullOptions{
		Source:        source.URL,
		AfterSequence: 0,
		Limit:         10,
	})
	var pullErr *CommandJournalPullError
	if !errors.As(err, &pullErr) || pullErr.Status != http.StatusBadGateway || !strings.Contains(err.Error(), "exceeds last sequence 0") {
		t.Fatalf("PullCommandJournal(past-last tail) error = %v/%#v, want 502 last sequence error", err, pullErr)
	}
	if result.Applied != 0 || result.AppliedThrough != 0 || result.LastSequence != 0 {
		t.Fatalf("past-last pull result = %#v, want no progress with last sequence 0", result)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("past-last pull applied value = %q, want empty", got)
	}
}

func TestPullCommandJournalReportsEmptyHasMoreTailAsBadGateway(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := CommandJournalTail{
			LastSequence: 2,
			HasMore:      true,
			Entries:      []CommandJournalRecord{},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}))
	defer source.Close()

	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	result, err := PullCommandJournal(context.Background(), ht, journal, CommandJournalPullOptions{
		Source:        source.URL,
		AfterSequence: 0,
		Limit:         10,
	})
	var pullErr *CommandJournalPullError
	if !errors.As(err, &pullErr) || pullErr.Status != http.StatusBadGateway || !strings.Contains(err.Error(), "without returning entries") {
		t.Fatalf("PullCommandJournal(empty has_more tail) error = %v/%#v, want 502 has_more error", err, pullErr)
	}
	if result.Applied != 0 || result.AppliedThrough != 0 || result.LastSequence != 2 || !result.HasMore {
		t.Fatalf("empty has_more pull result = %#v, want no progress with has_more preserved", result)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("empty has_more pull applied value = %q, want empty", got)
	}
}

func TestPullCommandJournalReportsCompactedTailAsConflict(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := CommandJournalTail{
			LastSequence:     3,
			CompactedThrough: 2,
			Entries: []CommandJournalRecord{
				{Sequence: 3, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}))
	defer source.Close()

	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	result, err := PullCommandJournal(context.Background(), ht, journal, CommandJournalPullOptions{
		Source:        source.URL,
		AfterSequence: 1,
		Limit:         10,
	})
	var pullErr *CommandJournalPullError
	if !errors.As(err, &pullErr) || pullErr.Status != http.StatusConflict || !errors.Is(err, ErrCommandJournalCompacted) {
		t.Fatalf("PullCommandJournal(compacted tail) error = %v/%#v, want 409 compacted error", err, pullErr)
	}
	if result.Applied != 0 || result.AppliedThrough != 1 || result.CompactedThrough != 2 {
		t.Fatalf("compacted pull result = %#v, want no progress with compacted boundary", result)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("compacted pull applied value = %q, want empty", got)
	}
}

func TestFetchCommandJournalTailDrainsErrorResponseBody(t *testing.T) {
	body := newTrackingReadCloser(strings.Repeat("journal source error ", 32))
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadGateway,
				Status:     "502 Bad Gateway",
				Header:     make(http.Header),
				Body:       body,
				Request:    request,
			}, nil
		}),
	}

	_, status, err := fetchCommandJournalTail(context.Background(), client, "http://source.example/api/journal")
	if err == nil || status != http.StatusBadGateway || !strings.Contains(err.Error(), "journal source returned 502 Bad Gateway") {
		t.Fatalf("fetchCommandJournalTail() status/error = %d/%v, want 502 source error", status, err)
	}
	if !body.drained || !body.closed {
		t.Fatalf("response body drained=%v closed=%v, want both true", body.drained, body.closed)
	}
}

func TestFetchCommandJournalTailMarksTruncatedErrorResponseBody(t *testing.T) {
	body := newTrackingReadCloser(strings.Repeat("x", maxCommandJournalErrorResponseBytes+128) + "tail-marker")
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadGateway,
				Status:     "502 Bad Gateway",
				Header:     make(http.Header),
				Body:       body,
				Request:    request,
			}, nil
		}),
	}

	_, status, err := fetchCommandJournalTail(context.Background(), client, "http://source.example/api/journal")
	if err == nil || status != http.StatusBadGateway || !strings.Contains(err.Error(), "journal source returned 502 Bad Gateway") {
		t.Fatalf("fetchCommandJournalTail() status/error = %d/%v, want 502 source error", status, err)
	}
	if strings.Contains(err.Error(), "tail-marker") {
		t.Fatal("error included body beyond limit")
	}
	if !strings.Contains(err.Error(), truncatedCommandJournalErrorResponseSuffix) {
		t.Fatalf("error = %q, want truncation suffix", err)
	}
	if len(err.Error()) > maxCommandJournalErrorResponseBytes+256 {
		t.Fatalf("error length = %d, want bounded body", len(err.Error()))
	}
	if !body.drained || !body.closed {
		t.Fatalf("response body drained=%v closed=%v, want both true", body.drained, body.closed)
	}
}

func TestFetchCommandJournalTailRejectsOversizedResponseBody(t *testing.T) {
	body := `{"entries":[],"padding":"` + strings.Repeat("x", maxCommandJournalTailResponseBytes) + `"}`
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    request,
			}, nil
		}),
	}

	_, status, err := fetchCommandJournalTail(context.Background(), client, "http://source.example/api/journal")
	if err == nil || status != http.StatusOK || !strings.Contains(err.Error(), "journal source response is too large") {
		t.Fatalf("fetchCommandJournalTail() status/error = %d/%v, want oversized response error", status, err)
	}
}

func TestDecodeCommandJournalTailResponseRejectsOversizedTrailingWhitespace(t *testing.T) {
	body := `{"entries":[]}` + strings.Repeat(" ", maxCommandJournalTailResponseBytes+1)
	_, err := decodeCommandJournalTailResponse(strings.NewReader(body))
	if !errors.Is(err, errCommandJournalTailResponseTooLarge) {
		t.Fatalf("decodeCommandJournalTailResponse() error = %v, want response too large", err)
	}
}

func TestFetchCommandJournalTailRejectsTrailingResponseJSON(t *testing.T) {
	client := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"entries":[]}{"entries":[]}`)),
				Request:    request,
			}, nil
		}),
	}

	_, status, err := fetchCommandJournalTail(context.Background(), client, "http://source.example/api/journal")
	if err == nil || status != http.StatusOK || !strings.Contains(err.Error(), "invalid trailing JSON") {
		t.Fatalf("fetchCommandJournalTail() status/error = %d/%v, want trailing JSON error", status, err)
	}
}
