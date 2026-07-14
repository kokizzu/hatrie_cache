package hatriecache

import (
	"context"
	"errors"
	"net/http"
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
