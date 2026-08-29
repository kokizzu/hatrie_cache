package hatPgWire

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
)

// CancelRegistry maps PostgreSQL backend key data to connection cancellation
// functions. It is safe for concurrent listener and cancel-request use.
type CancelRegistry struct {
	mu       sync.Mutex
	sessions map[uint32]cancelSession
}

type cancelSession struct {
	secret uint32
	cancel func()
}

// NewCancelRegistry creates an empty connection cancellation registry.
func NewCancelRegistry() *CancelRegistry {
	return &CancelRegistry{sessions: make(map[uint32]cancelSession)}
}

// Register records cancel and returns opaque PostgreSQL backend key data.
func (registry *CancelRegistry) Register(cancel func()) (uint32, uint32) {
	if registry == nil || cancel == nil {
		return 0, 0
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for {
		processID := randomCancelKey()
		if processID == 0 {
			continue
		}
		if _, exists := registry.sessions[processID]; exists {
			continue
		}
		secret := randomCancelKey()
		registry.sessions[processID] = cancelSession{secret: secret, cancel: cancel}
		return processID, secret
	}
}

// Unregister removes one backend key so it can no longer cancel a connection.
func (registry *CancelRegistry) Unregister(processID uint32) {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	delete(registry.sessions, processID)
	registry.mu.Unlock()
}

// Cancel verifies backend key data and invokes its matching cancellation.
func (registry *CancelRegistry) Cancel(processID uint32, secret uint32) bool {
	if registry == nil {
		return false
	}
	registry.mu.Lock()
	session, exists := registry.sessions[processID]
	registry.mu.Unlock()
	if !exists || session.secret != secret {
		return false
	}
	session.cancel()
	return true
}

func randomCancelKey() uint32 {
	var bytes [4]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return 0
	}
	return binary.BigEndian.Uint32(bytes[:])
}
