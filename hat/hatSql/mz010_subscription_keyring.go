package hatSql

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
)

// MaxSQLSubscriptionWirePreviousKeys bounds the grace window accepted during
// an HMAC key rotation. A bounded window prevents untrusted input from
// multiplying verification work without changing the wire format.
const MaxSQLSubscriptionWirePreviousKeys = 4

var ErrSQLSubscriptionWireKeyringInvalid = errors.New("hatSql: invalid SQL subscription wire keyring")

type sqlSubscriptionWireKeyringState struct {
	active   []byte
	previous [][]byte
}

// SQLSubscriptionWireKeyring seals with one active key and accepts a bounded
// set of previous keys while a transport fleet rotates credentials. The state
// is immutable after publication, so Open and Seal do not take a lock.
type SQLSubscriptionWireKeyring struct {
	state atomic.Pointer[sqlSubscriptionWireKeyringState]
	mu    sync.Mutex
}

// NewSQLSubscriptionWireKeyring creates a bounded keyring. Previous keys are
// tried only after the active key fails authentication; malformed envelopes
// still fail immediately.
func NewSQLSubscriptionWireKeyring(active []byte, previous ...[]byte) (*SQLSubscriptionWireKeyring, error) {
	state, ok := newSQLSubscriptionWireKeyringState(active, previous)
	if !ok {
		return nil, ErrSQLSubscriptionWireKeyringInvalid
	}
	ring := &SQLSubscriptionWireKeyring{}
	ring.state.Store(state)
	return ring, nil
}

// Seal encodes with the key that was active when the operation began.
func (ring *SQLSubscriptionWireKeyring) Seal(envelope SQLSubscriptionWireEnvelope) ([]byte, error) {
	if ring == nil {
		return nil, ErrSQLSubscriptionWireKeyringInvalid
	}
	state := ring.state.Load()
	if state == nil {
		return nil, ErrSQLSubscriptionWireKeyringInvalid
	}
	return SealSQLSubscriptionWireEnvelope(state.active, envelope)
}

// Open authenticates with the active key and then each retained previous key.
// A malformed frame is returned immediately, while an authenticated frame
// from any retained key is accepted.
func (ring *SQLSubscriptionWireKeyring) Open(wire []byte) (SQLSubscriptionWireEnvelope, error) {
	if ring == nil {
		return SQLSubscriptionWireEnvelope{}, ErrSQLSubscriptionWireKeyringInvalid
	}
	state := ring.state.Load()
	if state == nil {
		return SQLSubscriptionWireEnvelope{}, ErrSQLSubscriptionWireKeyringInvalid
	}
	envelope, err := OpenSQLSubscriptionWireEnvelope(state.active, wire)
	if err == nil {
		return envelope, nil
	}
	if !errors.Is(err, ErrSQLSubscriptionWireEnvelopeAuthentication) {
		return SQLSubscriptionWireEnvelope{}, err
	}
	lastErr := err
	for _, key := range state.previous {
		envelope, err = OpenSQLSubscriptionWireEnvelope(key, wire)
		if err == nil {
			return envelope, nil
		}
		if !errors.Is(err, ErrSQLSubscriptionWireEnvelopeAuthentication) {
			return SQLSubscriptionWireEnvelope{}, err
		}
		lastErr = err
	}
	return SQLSubscriptionWireEnvelope{}, lastErr
}

// Rotate publishes a new active key and retains the previous active key at
// the front of the bounded grace window. Key material is copied before it is
// published, so callers may safely reuse or clear their input buffers.
func (ring *SQLSubscriptionWireKeyring) Rotate(active []byte) error {
	if ring == nil || !validSQLSubscriptionWireKey(active) {
		return ErrSQLSubscriptionWireKeyringInvalid
	}
	ring.mu.Lock()
	defer ring.mu.Unlock()
	current := ring.state.Load()
	if current == nil {
		return ErrSQLSubscriptionWireKeyringInvalid
	}
	if bytes.Equal(current.active, active) {
		return nil
	}
	previous := make([][]byte, 0, MaxSQLSubscriptionWirePreviousKeys)
	previous = append(previous, activeKeyClone(current.active))
	for _, key := range current.previous {
		if len(previous) >= MaxSQLSubscriptionWirePreviousKeys {
			break
		}
		if bytes.Equal(key, active) || sqlSubscriptionWireKeyringContains(previous, key) {
			continue
		}
		previous = append(previous, activeKeyClone(key))
	}
	next, ok := newSQLSubscriptionWireKeyringState(active, previous)
	if !ok {
		return ErrSQLSubscriptionWireKeyringInvalid
	}
	ring.state.Store(next)
	return nil
}

func newSQLSubscriptionWireKeyringState(active []byte, previous [][]byte) (*sqlSubscriptionWireKeyringState, bool) {
	if !validSQLSubscriptionWireKey(active) || len(previous) > MaxSQLSubscriptionWirePreviousKeys {
		return nil, false
	}
	state := &sqlSubscriptionWireKeyringState{active: activeKeyClone(active)}
	for _, key := range previous {
		if !validSQLSubscriptionWireKey(key) || bytes.Equal(key, active) || sqlSubscriptionWireKeyringContains(state.previous, key) {
			if bytes.Equal(key, active) || sqlSubscriptionWireKeyringContains(state.previous, key) {
				continue
			}
			return nil, false
		}
		state.previous = append(state.previous, activeKeyClone(key))
	}
	return state, true
}

func sqlSubscriptionWireKeyringContains(keys [][]byte, wanted []byte) bool {
	for _, key := range keys {
		if bytes.Equal(key, wanted) {
			return true
		}
	}
	return false
}

func activeKeyClone(key []byte) []byte {
	return append([]byte(nil), key...)
}
