package hatriecache

import (
	"crypto/subtle"
	"strings"
	"time"
)

const authBearerPrefix = "Bearer "

type authTokenSet struct {
	current           string
	previous          string
	previousExpiresAt time.Time
}

func newAuthTokenSet(current string, previous string, previousExpiresAt time.Time) authTokenSet {
	return authTokenSet{
		current:           normalizeAuthToken(current),
		previous:          normalizeAuthToken(previous),
		previousExpiresAt: previousExpiresAt,
	}
}

func (tokens authTokenSet) configured() bool {
	return tokens.current != "" || tokens.previous != ""
}

func (tokens authTokenSet) matches(candidate string, now time.Time) bool {
	if tokens.current != "" && authTokenMatches(candidate, tokens.current) {
		return true
	}
	return tokens.previous != "" &&
		!tokens.previousExpiresAt.IsZero() &&
		now.Before(tokens.previousExpiresAt) &&
		authTokenMatches(candidate, tokens.previous)
}

func normalizeAuthToken(token string) string {
	return strings.TrimSpace(token)
}

func authTokenMatches(candidate string, token string) bool {
	token = normalizeAuthToken(token)
	if token == "" {
		return true
	}
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(candidate), []byte(token)) == 1
}

func authBearerToken(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < len(authBearerPrefix) || !strings.EqualFold(value[:len(authBearerPrefix)], authBearerPrefix) {
		return ""
	}
	return strings.TrimSpace(value[len(authBearerPrefix):])
}
