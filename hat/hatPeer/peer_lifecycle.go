package hatPeer

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrPeerLifecycleOptionsInvalid indicates an invalid registry bound.
	ErrPeerLifecycleOptionsInvalid = errors.New("hatPeer: peer lifecycle options are invalid")
	// ErrPeerLifecycleKindInvalid indicates an unknown lifecycle event kind.
	ErrPeerLifecycleKindInvalid = errors.New("hatPeer: peer lifecycle kind is invalid")
	// ErrPeerLifecycleHookRequired indicates that a hook was nil.
	ErrPeerLifecycleHookRequired = errors.New("hatPeer: peer lifecycle hook is required")
	// ErrPeerLifecycleRegistryNil indicates a method call on a nil registry.
	ErrPeerLifecycleRegistryNil = errors.New("hatPeer: peer lifecycle registry is nil")
	// ErrPeerLifecycleHookLimit indicates that the registry is full.
	ErrPeerLifecycleHookLimit = errors.New("hatPeer: peer lifecycle hook limit reached")
)

const (
	// DefaultPeerLifecycleMaxHooks bounds registered hooks per registry.
	DefaultPeerLifecycleMaxHooks = 64
	// DefaultPeerLifecycleHistoryLimit bounds retained event history per registry.
	DefaultPeerLifecycleHistoryLimit = 32
	maxPeerLifecycleHooks            = 4096
	maxPeerLifecycleHistory          = 4096
)

// PeerLifecycleKind identifies a peer connection or schema lifecycle event.
type PeerLifecycleKind uint8

const (
	PeerLifecycleConnected PeerLifecycleKind = iota + 1
	PeerLifecycleDisconnected
	PeerLifecycleShutdown
	PeerLifecycleSchemaReloaded
	peerLifecycleKindCount
)

// String returns the stable name of a lifecycle kind.
func (kind PeerLifecycleKind) String() string {
	switch kind {
	case PeerLifecycleConnected:
		return "connected"
	case PeerLifecycleDisconnected:
		return "disconnected"
	case PeerLifecycleShutdown:
		return "shutdown"
	case PeerLifecycleSchemaReloaded:
		return "schema_reloaded"
	default:
		return "unknown"
	}
}

func (kind PeerLifecycleKind) valid() bool {
	return kind > 0 && kind < peerLifecycleKindCount
}

// PeerLifecycleEvent is the immutable value passed to a lifecycle hook.
// Error contains a bounded terminal error string for disconnect/shutdown
// events when the session ended with an error.
type PeerLifecycleEvent struct {
	Kind   PeerLifecycleKind `json:"kind"`
	PeerID string            `json:"peer_id,omitempty"`
	At     time.Time         `json:"at"`
	Error  string            `json:"error,omitempty"`
}

// PeerLifecycleHook receives one lifecycle event. Hooks run synchronously in
// the caller of Emit, outside the registry lock. A panic in one hook is
// recovered so it cannot prevent later hooks from receiving the event.
type PeerLifecycleHook func(PeerLifecycleEvent)

// PeerLifecycleOptions bounds registry memory and callback count. Zero values
// select the documented defaults.
type PeerLifecycleOptions struct {
	MaxHooks     int
	HistoryLimit int
}

type peerLifecycleHookEntry struct {
	id   uint64
	hook PeerLifecycleHook
}

// PeerLifecycleRegistry stores bounded hooks and recent lifecycle events. It
// has no package-global state and is safe for concurrent use.
type PeerLifecycleRegistry struct {
	mu           sync.Mutex
	maxHooks     int
	historyLimit int
	nextID       uint64
	hookCount    int
	hooks        [peerLifecycleKindCount][]peerLifecycleHookEntry
	history      []PeerLifecycleEvent
	historyStart int
	historySize  int
}

// NewPeerLifecycleRegistry creates a bounded lifecycle registry.
func NewPeerLifecycleRegistry(options PeerLifecycleOptions) (*PeerLifecycleRegistry, error) {
	maxHooks := options.MaxHooks
	if maxHooks == 0 {
		maxHooks = DefaultPeerLifecycleMaxHooks
	}
	historyLimit := options.HistoryLimit
	if historyLimit == 0 {
		historyLimit = DefaultPeerLifecycleHistoryLimit
	}
	if maxHooks < 1 || maxHooks > maxPeerLifecycleHooks || historyLimit < 1 || historyLimit > maxPeerLifecycleHistory {
		return nil, ErrPeerLifecycleOptionsInvalid
	}
	return &PeerLifecycleRegistry{
		maxHooks:     maxHooks,
		historyLimit: historyLimit,
		history:      make([]PeerLifecycleEvent, historyLimit),
	}, nil
}

// Register adds a hook and returns its registry-local ID.
func (registry *PeerLifecycleRegistry) Register(kind PeerLifecycleKind, hook PeerLifecycleHook) (uint64, error) {
	if registry == nil {
		return 0, ErrPeerLifecycleRegistryNil
	}
	if !kind.valid() {
		return 0, ErrPeerLifecycleKindInvalid
	}
	if hook == nil {
		return 0, ErrPeerLifecycleHookRequired
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.hookCount >= registry.maxHooks {
		return 0, ErrPeerLifecycleHookLimit
	}
	registry.nextID++
	if registry.nextID == 0 {
		registry.nextID++
	}
	entry := peerLifecycleHookEntry{id: registry.nextID, hook: hook}
	registry.hooks[kind] = append(registry.hooks[kind], entry)
	registry.hookCount++
	return entry.id, nil
}

// Unregister removes a hook. It returns false when the hook does not exist.
func (registry *PeerLifecycleRegistry) Unregister(kind PeerLifecycleKind, id uint64) bool {
	if registry == nil || !kind.valid() || id == 0 {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entries := registry.hooks[kind]
	for index, entry := range entries {
		if entry.id != id {
			continue
		}
		copy(entries[index:], entries[index+1:])
		entries[len(entries)-1] = peerLifecycleHookEntry{}
		registry.hooks[kind] = entries[:len(entries)-1]
		registry.hookCount--
		return true
	}
	return false
}

// Emit records and delivers an event in hook registration order.
func (registry *PeerLifecycleRegistry) Emit(event PeerLifecycleEvent) error {
	if registry == nil {
		return ErrPeerLifecycleRegistryNil
	}
	if !event.Kind.valid() {
		return ErrPeerLifecycleKindInvalid
	}
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	} else {
		event.At = event.At.UTC()
	}
	registry.mu.Lock()
	if registry.historySize < registry.historyLimit {
		index := (registry.historyStart + registry.historySize) % registry.historyLimit
		registry.history[index] = event
		registry.historySize++
	} else {
		registry.history[registry.historyStart] = event
		registry.historyStart = (registry.historyStart + 1) % registry.historyLimit
	}
	entries := registry.hooks[event.Kind]
	if len(entries) == 0 {
		registry.mu.Unlock()
		return nil
	}
	if len(entries) == 1 {
		hook := entries[0].hook
		registry.mu.Unlock()
		invokePeerLifecycleHook(hook, event)
		return nil
	}
	entries = append([]peerLifecycleHookEntry(nil), entries...)
	registry.mu.Unlock()
	for _, entry := range entries {
		invokePeerLifecycleHook(entry.hook, event)
	}
	return nil
}

func invokePeerLifecycleHook(hook PeerLifecycleHook, event PeerLifecycleEvent) {
	defer func() { _ = recover() }()
	hook(event)
}

// Snapshot returns recent events in chronological order.
func (registry *PeerLifecycleRegistry) Snapshot() []PeerLifecycleEvent {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.historySize == 0 {
		return nil
	}
	events := make([]PeerLifecycleEvent, registry.historySize)
	for index := range events {
		events[index] = registry.history[(registry.historyStart+index)%registry.historyLimit]
	}
	return events
}
