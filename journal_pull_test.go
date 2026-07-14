package hatriecache

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
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
