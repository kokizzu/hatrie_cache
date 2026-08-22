package hatAuth

import (
	"testing"
	"time"
)

func TestTokenSetAcceptsCurrentAndUnexpiredPreviousTokens(t *testing.T) {
	expiresAt := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	tokens := NewTokenSet(" current-token ", " previous-token ", expiresAt)

	if !tokens.Configured() {
		t.Fatal("Configured() = false, want true")
	}
	for _, candidate := range []string{"current-token", " previous-token "} {
		if !tokens.Matches(candidate, expiresAt.Add(-time.Nanosecond)) {
			t.Fatalf("Matches(%q) = false, want true", candidate)
		}
	}
	if tokens.Matches("previous-token", expiresAt) {
		t.Fatal("Matches(previous-token at expiry) = true, want false")
	}
	if tokens.Matches("wrong-token", expiresAt.Add(-time.Nanosecond)) {
		t.Fatal("Matches(wrong-token) = true, want false")
	}
}

func TestBearerTokenAcceptsBearerSchemeOnly(t *testing.T) {
	for _, test := range []struct {
		value string
		want  string
	}{
		{value: "Bearer token", want: "token"},
		{value: " bearer  token ", want: "token"},
		{value: "Basic token", want: ""},
		{value: "Bearer", want: ""},
		{value: "", want: ""},
	} {
		if got := BearerToken(test.value); got != test.want {
			t.Errorf("BearerToken(%q) = %q, want %q", test.value, got, test.want)
		}
	}
}
