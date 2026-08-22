// Package authn provides shared authentication token handling for transport
// boundaries. It intentionally contains no cache or protocol dependencies.
package authn

import (
	"crypto/subtle"
	"strings"
	"time"
)

const bearerPrefix = "Bearer "

// TokenSet accepts a current token and, optionally, an expiring previous token
// during credential rotation.
type TokenSet struct {
	current           string
	previous          string
	previousExpiresAt time.Time
}

func NewTokenSet(current string, previous string, previousExpiresAt time.Time) TokenSet {
	return TokenSet{
		current:           Normalize(current),
		previous:          Normalize(previous),
		previousExpiresAt: previousExpiresAt,
	}
}

func (tokens TokenSet) Configured() bool {
	return tokens.current != "" || tokens.previous != ""
}

func (tokens TokenSet) Matches(candidate string, now time.Time) bool {
	if tokens.current != "" && tokenMatches(candidate, tokens.current) {
		return true
	}
	return tokens.previous != "" &&
		!tokens.previousExpiresAt.IsZero() &&
		now.Before(tokens.previousExpiresAt) &&
		tokenMatches(candidate, tokens.previous)
}

func Normalize(token string) string {
	return strings.TrimSpace(token)
}

func tokenMatches(candidate string, token string) bool {
	token = Normalize(token)
	if token == "" {
		return true
	}
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(candidate), []byte(token)) == 1
}

// BearerToken returns the token from an HTTP or gRPC Authorization value.
func BearerToken(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < len(bearerPrefix) || !strings.EqualFold(value[:len(bearerPrefix)], bearerPrefix) {
		return ""
	}
	return strings.TrimSpace(value[len(bearerPrefix):])
}
