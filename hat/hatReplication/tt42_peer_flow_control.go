package hatReplication

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultPeerRelayBackpressureMaxPeers bounds retained per-peer state.
	DefaultPeerRelayBackpressureMaxPeers = 1024
	// DefaultPeerRelayBackpressureMaxPeerNameBytes bounds one peer identity.
	DefaultPeerRelayBackpressureMaxPeerNameBytes = 256
	maxPeerRelayBackpressurePeers                = 100_000
	maxPeerRelayBackpressurePeerNameBytes        = 4096
)

var (
	ErrPeerRelayBackpressureNil             = errors.New("peer relay backpressure controller is nil")
	ErrPeerRelayBackpressureInvalidOptions  = errors.New("peer relay backpressure options are invalid")
	ErrPeerRelayBackpressurePeerRequired    = errors.New("peer relay backpressure peer is required")
	ErrPeerRelayBackpressurePeerNameTooLong = errors.New("peer relay backpressure peer name is too long")
	ErrPeerRelayBackpressurePeerLimit       = errors.New("peer relay backpressure peer limit reached")
)

// PeerRelayBackpressureOptions configures independent lag hysteresis for a
// bounded set of downstream peers. Disabled is the zero-value behavior.
type PeerRelayBackpressureOptions struct {
	Enabled          bool
	HighWatermark    uint64
	ResumeWatermark  uint64
	MaxPeers         int
	MaxPeerNameBytes int
}

// PeerRelayBackpressureDecision describes one peer's admission decision.
type PeerRelayBackpressureDecision struct {
	Peer         string
	Allowed      bool
	Paused       bool
	Transitioned bool
	Lag          uint64
	Transitions  uint64
}

// PeerRelayBackpressurePeerSnapshot is the bounded state for one peer.
type PeerRelayBackpressurePeerSnapshot struct {
	Peer        string
	Paused      bool
	Lag         uint64
	Transitions uint64
}

// PeerRelayBackpressureSnapshot is a deterministic monitoring snapshot.
type PeerRelayBackpressureSnapshot struct {
	Enabled          bool
	HighWatermark    uint64
	ResumeWatermark  uint64
	MaxPeers         int
	MaxPeerNameBytes int
	Peers            []PeerRelayBackpressurePeerSnapshot
}

type peerRelayBackpressureState struct {
	paused      bool
	lag         uint64
	transitions uint64
}

// PeerRelayBackpressure applies the existing lag hysteresis policy separately
// to each downstream peer. It owns no queue and never drops data; callers
// decide how to retain or reject work after Observe returns false.
type PeerRelayBackpressure struct {
	mu               sync.Mutex
	enabled          bool
	highWatermark    uint64
	resumeWatermark  uint64
	maxPeers         int
	maxPeerNameBytes int
	peers            map[string]peerRelayBackpressureState
}

// NewPeerRelayBackpressure creates a bounded per-peer controller. Zero numeric
// options select the existing relay watermarks and conservative cardinality
// limits.
func NewPeerRelayBackpressure(options PeerRelayBackpressureOptions) (*PeerRelayBackpressure, error) {
	if options.MaxPeers < 0 || options.MaxPeers > maxPeerRelayBackpressurePeers {
		return nil, ErrPeerRelayBackpressureInvalidOptions
	}
	if options.MaxPeerNameBytes < 0 || options.MaxPeerNameBytes > maxPeerRelayBackpressurePeerNameBytes {
		return nil, ErrPeerRelayBackpressureInvalidOptions
	}
	highWatermark := options.HighWatermark
	if highWatermark == 0 {
		highWatermark = DefaultRelayBackpressureHighWatermark
	}
	resumeWatermark := options.ResumeWatermark
	if resumeWatermark == 0 {
		resumeWatermark = DefaultRelayBackpressureResumeWatermark
		if resumeWatermark >= highWatermark {
			resumeWatermark = highWatermark / 2
		}
	}
	if resumeWatermark >= highWatermark {
		resumeWatermark = highWatermark - 1
	}
	maxPeers := options.MaxPeers
	if maxPeers == 0 {
		maxPeers = DefaultPeerRelayBackpressureMaxPeers
	}
	maxPeerNameBytes := options.MaxPeerNameBytes
	if maxPeerNameBytes == 0 {
		maxPeerNameBytes = DefaultPeerRelayBackpressureMaxPeerNameBytes
	}
	controller := &PeerRelayBackpressure{
		enabled:          options.Enabled,
		highWatermark:    highWatermark,
		resumeWatermark:  resumeWatermark,
		maxPeers:         maxPeers,
		maxPeerNameBytes: maxPeerNameBytes,
	}
	if controller.enabled {
		controller.peers = make(map[string]peerRelayBackpressureState, maxPeers)
	}
	return controller, nil
}

// Observe records a peer's current downstream lag and returns its admission
// decision. A new peer consumes one bounded map entry only when enabled.
func (controller *PeerRelayBackpressure) Observe(peer string, lag uint64) (PeerRelayBackpressureDecision, error) {
	if controller == nil {
		return PeerRelayBackpressureDecision{}, ErrPeerRelayBackpressureNil
	}
	peer, err := controller.normalizePeer(peer)
	if err != nil {
		return PeerRelayBackpressureDecision{}, err
	}
	if !controller.enabled {
		return PeerRelayBackpressureDecision{Peer: peer, Allowed: true, Lag: lag}, nil
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	state, exists := controller.peers[peer]
	if !exists {
		if len(controller.peers) >= controller.maxPeers {
			return PeerRelayBackpressureDecision{}, fmt.Errorf("%w: maximum is %d", ErrPeerRelayBackpressurePeerLimit, controller.maxPeers)
		}
	}
	state.lag = lag
	transitioned := false
	if state.paused {
		if lag <= controller.resumeWatermark {
			state.paused = false
			state.transitions++
			transitioned = true
		}
	} else if lag >= controller.highWatermark {
		state.paused = true
		state.transitions++
		transitioned = true
	}
	controller.peers[peer] = state
	return PeerRelayBackpressureDecision{
		Peer:         peer,
		Allowed:      !state.paused,
		Paused:       state.paused,
		Transitioned: transitioned,
		Lag:          lag,
		Transitions:  state.transitions,
	}, nil
}

func (controller *PeerRelayBackpressure) normalizePeer(peer string) (string, error) {
	peer = strings.TrimSpace(peer)
	if peer == "" {
		return "", ErrPeerRelayBackpressurePeerRequired
	}
	if len(peer) > controller.maxPeerNameBytes {
		return "", fmt.Errorf("%w: maximum is %d bytes", ErrPeerRelayBackpressurePeerNameTooLong, controller.maxPeerNameBytes)
	}
	return peer, nil
}

// Remove forgets one peer's state and reports whether it existed.
func (controller *PeerRelayBackpressure) Remove(peer string) bool {
	if controller == nil {
		return false
	}
	peer = strings.TrimSpace(peer)
	if peer == "" || len(peer) > controller.maxPeerNameBytes || !controller.enabled {
		return false
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if _, exists := controller.peers[peer]; !exists {
		return false
	}
	delete(controller.peers, peer)
	return true
}

// Snapshot returns sorted per-peer state without exposing the internal map.
func (controller *PeerRelayBackpressure) Snapshot() PeerRelayBackpressureSnapshot {
	if controller == nil {
		return PeerRelayBackpressureSnapshot{}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	snapshot := PeerRelayBackpressureSnapshot{
		Enabled:          controller.enabled,
		HighWatermark:    controller.highWatermark,
		ResumeWatermark:  controller.resumeWatermark,
		MaxPeers:         controller.maxPeers,
		MaxPeerNameBytes: controller.maxPeerNameBytes,
		Peers:            make([]PeerRelayBackpressurePeerSnapshot, 0, len(controller.peers)),
	}
	for peer, state := range controller.peers {
		snapshot.Peers = append(snapshot.Peers, PeerRelayBackpressurePeerSnapshot{
			Peer:        peer,
			Paused:      state.paused,
			Lag:         state.lag,
			Transitions: state.transitions,
		})
	}
	sort.Slice(snapshot.Peers, func(left, right int) bool {
		return snapshot.Peers[left].Peer < snapshot.Peers[right].Peer
	})
	return snapshot
}
