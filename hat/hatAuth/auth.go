// Package hatAuth provides shared authentication token handling for transport
// boundaries. It intentionally contains no cache or protocol dependencies.
package hatAuth

import (
	"crypto/subtle"
	"errors"
	"strings"
	"sync/atomic"
	"time"
)

const bearerPrefix = "Bearer "

// ErrTokenRotationInvalid reports a rotation that would leave the rotator
// without credentials or retain an overlap token without an expiry.
var ErrTokenRotationInvalid = errors.New("hatAuth: token rotation is invalid")

// TokenSet accepts a current token and, optionally, an expiring previous token
// during credential rotation.
type TokenSet struct {
	current           string
	previous          string
	previousExpiresAt time.Time
}

// TokenRotator publishes immutable token snapshots atomically. Authentication
// reads never block rotations and do not allocate; only Rotate allocates the
// next snapshot. A nil rotator never authenticates.
type TokenRotator struct {
	value atomic.Pointer[TokenSet]
}

// NewTokenRotator creates a live credential set. The previous token is
// accepted only before previousExpiresAt; use an empty previous token when no
// overlap is required.
func NewTokenRotator(current, previous string, previousExpiresAt time.Time) *TokenRotator {
	rotator := &TokenRotator{}
	snapshot := NewTokenSet(current, previous, previousExpiresAt)
	rotator.value.Store(&snapshot)
	return rotator
}

// Rotate atomically replaces the active credentials. A non-empty previous
// token must have a non-zero expiry so an overlap cannot become permanent.
func (rotator *TokenRotator) Rotate(current, previous string, previousExpiresAt time.Time) error {
	if rotator == nil {
		return ErrTokenRotationInvalid
	}
	snapshot := NewTokenSet(current, previous, previousExpiresAt)
	if !snapshot.Configured() || (snapshot.previous != "" && snapshot.previousExpiresAt.IsZero()) {
		return ErrTokenRotationInvalid
	}
	rotator.value.Store(&snapshot)
	return nil
}

// Configured reports whether the current snapshot has at least one token.
func (rotator *TokenRotator) Configured() bool {
	if rotator == nil {
		return false
	}
	snapshot := rotator.value.Load()
	return snapshot != nil && snapshot.Configured()
}

// Matches checks a candidate against the current immutable snapshot.
func (rotator *TokenRotator) Matches(candidate string, now time.Time) bool {
	if rotator == nil {
		return false
	}
	snapshot := rotator.value.Load()
	return snapshot != nil && snapshot.Matches(candidate, now)
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
