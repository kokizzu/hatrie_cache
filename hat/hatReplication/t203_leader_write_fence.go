package hatReplication

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const maxReplicaSetLeaderWriteFenceNodeBytes = 256

var (
	// ErrReplicaSetLeaderWriteFenceNil indicates a method call on a nil fence.
	ErrReplicaSetLeaderWriteFenceNil = errors.New("hatReplication: replica-set leader write fence is nil")
	// ErrReplicaSetLeaderWriteFenceInvalid identifies malformed configuration,
	// transition, write, or callback input.
	ErrReplicaSetLeaderWriteFenceInvalid = errors.New("hatReplication: replica-set leader write fence input is invalid")
	// ErrReplicaSetLeaderWriteFenceDisabled indicates that the opt-in fence is
	// not enabled.
	ErrReplicaSetLeaderWriteFenceDisabled = errors.New("hatReplication: replica-set leader write fence is disabled")
	// ErrReplicaSetLeaderWriteFenceStale identifies a stale writer or a
	// transition that would move term or fencing token backward or sideways.
	ErrReplicaSetLeaderWriteFenceStale = errors.New("hatReplication: replica-set leader write fence is stale")
	// ErrReplicaSetLeaderWriteFenceNoLeader indicates that no leader has been
	// installed yet, so no write can be admitted.
	ErrReplicaSetLeaderWriteFenceNoLeader = errors.New("hatReplication: replica-set leader write fence has no leader")
	// ErrReplicaSetLeaderWriteFenceGeneration indicates that the local
	// transition generation cannot advance.
	ErrReplicaSetLeaderWriteFenceGeneration = errors.New("hatReplication: replica-set leader write fence generation is exhausted")
)

// ReplicaSetLeaderWriteFenceOptions configures the local write gate. The
// zero value is disabled; when enabled, an empty initial leader is allowed so
// callers can install the first committed election result with Advance.
type ReplicaSetLeaderWriteFenceOptions struct {
	Enabled             bool
	InitialLeader       string
	InitialTerm         uint64
	InitialFencingToken uint64
}

// ReplicaSetLeaderWrite identifies the leadership credentials attached to one
// write. The caller must carry the same term and fencing token that was
// committed for the active leader.
type ReplicaSetLeaderWrite struct {
	NodeID       string
	Term         uint64
	FencingToken uint64
}

// ReplicaSetLeaderWriteFenceTransition installs a new committed leader. Term
// and FencingToken must both advance strictly, which prevents an old leader
// from reusing either half of a prior credential pair.
type ReplicaSetLeaderWriteFenceTransition struct {
	LeaderID     string
	Term         uint64
	FencingToken uint64
}

// ReplicaSetLeaderWriteFenceState is a detached, monotone fence snapshot.
type ReplicaSetLeaderWriteFenceState struct {
	LeaderID     string
	Term         uint64
	FencingToken uint64
	Generation   uint64
}

// ReplicaSetLeaderWriteFence serializes committed-leader transitions with
// admitted write callbacks. Advance waits for an in-flight callback, so no
// callback admitted after a successful transition can use the prior leader
// credentials. The callback must perform the actual local write while the
// fence is held; remote stores must enforce the same credentials themselves.
type ReplicaSetLeaderWriteFence struct {
	mu      sync.RWMutex
	enabled bool
	state   ReplicaSetLeaderWriteFenceState
}

// NewReplicaSetLeaderWriteFence constructs an opt-in local stale-writer gate.
// Disabled options intentionally skip validation and preserve the default-off
// behavior of the existing replication paths.
func NewReplicaSetLeaderWriteFence(options ReplicaSetLeaderWriteFenceOptions) (*ReplicaSetLeaderWriteFence, error) {
	if !options.Enabled {
		return &ReplicaSetLeaderWriteFence{}, nil
	}
	leader, err := normalizeReplicaSetLeaderWriteFenceNode(options.InitialLeader)
	if err != nil {
		return nil, err
	}
	if leader == "" {
		if options.InitialTerm != 0 || options.InitialFencingToken != 0 {
			return nil, fmt.Errorf("%w: empty leader requires zero term and fencing token", ErrReplicaSetLeaderWriteFenceInvalid)
		}
	} else if options.InitialTerm == 0 || options.InitialFencingToken == 0 {
		return nil, fmt.Errorf("%w: initial leader requires non-zero term and fencing token", ErrReplicaSetLeaderWriteFenceInvalid)
	}
	return &ReplicaSetLeaderWriteFence{
		enabled: true,
		state: ReplicaSetLeaderWriteFenceState{
			LeaderID:     leader,
			Term:         options.InitialTerm,
			FencingToken: options.InitialFencingToken,
			Generation:   1,
		},
	}, nil
}

// Enabled reports whether this fence admits writes and transitions.
func (fence *ReplicaSetLeaderWriteFence) Enabled() bool {
	return fence != nil && fence.enabled
}

// Advance installs a new leader credential pair. It is the local commit fence
// to call after the external election/consensus path has committed the same
// leader, term, and fencing token.
func (fence *ReplicaSetLeaderWriteFence) Advance(transition ReplicaSetLeaderWriteFenceTransition) (ReplicaSetLeaderWriteFenceState, error) {
	if fence == nil {
		return ReplicaSetLeaderWriteFenceState{}, ErrReplicaSetLeaderWriteFenceNil
	}
	if !fence.enabled {
		return ReplicaSetLeaderWriteFenceState{}, ErrReplicaSetLeaderWriteFenceDisabled
	}
	leader, err := normalizeReplicaSetLeaderWriteFenceNode(transition.LeaderID)
	if err != nil {
		return ReplicaSetLeaderWriteFenceState{}, err
	}
	if leader == "" || transition.Term == 0 || transition.FencingToken == 0 {
		return ReplicaSetLeaderWriteFenceState{}, fmt.Errorf("%w: transition requires leader, term, and fencing token", ErrReplicaSetLeaderWriteFenceInvalid)
	}
	fence.mu.Lock()
	defer fence.mu.Unlock()
	if transition.Term <= fence.state.Term || transition.FencingToken <= fence.state.FencingToken {
		return ReplicaSetLeaderWriteFenceState{}, fmt.Errorf("%w: term or fencing token did not advance", ErrReplicaSetLeaderWriteFenceStale)
	}
	if fence.state.Generation == ^uint64(0) {
		return ReplicaSetLeaderWriteFenceState{}, ErrReplicaSetLeaderWriteFenceGeneration
	}
	fence.state = ReplicaSetLeaderWriteFenceState{
		LeaderID:     leader,
		Term:         transition.Term,
		FencingToken: transition.FencingToken,
		Generation:   fence.state.Generation + 1,
	}
	return fence.state, nil
}

// Execute admits one write only when its credentials exactly match the
// committed leader. The callback runs while the read lock is held, so Advance
// cannot cross an admitted local write until the callback returns.
func (fence *ReplicaSetLeaderWriteFence) Execute(write ReplicaSetLeaderWrite, callback func() error) error {
	if fence == nil {
		return ErrReplicaSetLeaderWriteFenceNil
	}
	if !fence.enabled {
		return ErrReplicaSetLeaderWriteFenceDisabled
	}
	if callback == nil {
		return fmt.Errorf("%w: write callback is nil", ErrReplicaSetLeaderWriteFenceInvalid)
	}
	node, err := normalizeReplicaSetLeaderWriteFenceNode(write.NodeID)
	if err != nil {
		return err
	}
	fence.mu.RLock()
	defer fence.mu.RUnlock()
	if fence.state.LeaderID == "" {
		return ErrReplicaSetLeaderWriteFenceNoLeader
	}
	if node != fence.state.LeaderID || write.Term != fence.state.Term || write.FencingToken != fence.state.FencingToken {
		return ErrReplicaSetLeaderWriteFenceStale
	}
	return callback()
}

// Snapshot returns the detached current fence state.
func (fence *ReplicaSetLeaderWriteFence) Snapshot() ReplicaSetLeaderWriteFenceState {
	if fence == nil {
		return ReplicaSetLeaderWriteFenceState{}
	}
	fence.mu.RLock()
	defer fence.mu.RUnlock()
	return fence.state
}

func normalizeReplicaSetLeaderWriteFenceNode(node string) (string, error) {
	node = strings.TrimSpace(node)
	if node == "" {
		return "", nil
	}
	if len(node) > maxReplicaSetLeaderWriteFenceNodeBytes || strings.IndexByte(node, 0) >= 0 {
		return "", fmt.Errorf("%w: leader identity is invalid", ErrReplicaSetLeaderWriteFenceInvalid)
	}
	return node, nil
}
