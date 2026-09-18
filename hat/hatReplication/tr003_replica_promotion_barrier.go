package hatReplication

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// DefaultReplicaPromotionBarrierMaxReplicas bounds the tracked replica set
	// when no explicit limit is supplied.
	DefaultReplicaPromotionBarrierMaxReplicas = 64
	maxReplicaPromotionBarrierReplicas        = 1 << 20
)

var (
	// ErrReplicaPromotionBarrierNil indicates a method call on a nil barrier.
	ErrReplicaPromotionBarrierNil = errors.New("hatriecache: replica promotion barrier is nil")
	// ErrReplicaPromotionBarrierOptionsInvalid indicates an invalid replica bound.
	ErrReplicaPromotionBarrierOptionsInvalid = errors.New("hatriecache: replica promotion barrier options are invalid")
	// ErrReplicaPromotionBarrierInvalidNode indicates an empty or non-canonical node ID.
	ErrReplicaPromotionBarrierInvalidNode = errors.New("hatriecache: replica promotion barrier node is invalid")
	// ErrReplicaPromotionBarrierSequenceRegression indicates a lower observed sequence.
	ErrReplicaPromotionBarrierSequenceRegression = errors.New("hatriecache: replica promotion barrier sequence regressed")
	// ErrReplicaPromotionBarrierNotCaughtUp indicates that a replica is below the source fence.
	ErrReplicaPromotionBarrierNotCaughtUp = errors.New("hatriecache: replica is not caught up to the promotion fence")
	// ErrReplicaPromotionBarrierSourceAdvanced indicates that the source moved after capture.
	ErrReplicaPromotionBarrierSourceAdvanced = errors.New("hatriecache: promotion source advanced after capture")
	// ErrReplicaPromotionBarrierStaleToken indicates that a previous promotion already advanced the generation.
	ErrReplicaPromotionBarrierStaleToken = errors.New("hatriecache: replica promotion token is stale")
	// ErrReplicaPromotionBarrierGenerationExhausted indicates that no next generation exists.
	ErrReplicaPromotionBarrierGenerationExhausted = errors.New("hatriecache: replica promotion generation is exhausted")
)

// ReplicaPromotionBarrierOptions bounds the number of replicas whose applied
// journal sequences are tracked.
type ReplicaPromotionBarrierOptions struct {
	// MaxReplicas is the maximum number of tracked replica IDs. Zero uses
	// DefaultReplicaPromotionBarrierMaxReplicas.
	MaxReplicas int
}

// ReplicaPromotionProgress is one replica's latest applied journal sequence.
type ReplicaPromotionProgress struct {
	Node            string `json:"node"`
	AppliedSequence uint64 `json:"applied_sequence"`
}

// ReplicaPromotionBarrierSnapshot is a detached progress view.
type ReplicaPromotionBarrierSnapshot struct {
	Generation     uint64                     `json:"generation"`
	SourceSequence uint64                     `json:"source_sequence"`
	Replicas       []ReplicaPromotionProgress `json:"replicas"`
}

// ReplicaPromotionToken captures the exact source sequence a replica must
// have applied before promotion. A token is single-use: Promote advances the
// generation after accepting it.
type ReplicaPromotionToken struct {
	Generation       uint64
	Node             string
	RequiredSequence uint64
	AppliedSequence  uint64
}

// ReplicaPromotionResult records a successful promotion barrier commit.
type ReplicaPromotionResult struct {
	Generation      uint64
	Node            string
	SourceSequence  uint64
	AppliedSequence uint64
}

// ReplicaPromotionBarrier is a local, transport-neutral catch-up barrier for
// replica promotion. Observe the source and replicas from the replication
// control plane, Capture after the old source is fenced, and Promote exactly
// once. It does not implement elections, writes, or cross-process fencing.
// Do not copy a barrier after first use.
type ReplicaPromotionBarrier struct {
	maxReplicas int
	updateMu    sync.Mutex
	generation  atomic.Uint64
	state       atomic.Pointer[replicaPromotionBarrierState]
}

type replicaPromotionBarrierState struct {
	sourceSequence uint64
	replicas       map[string]uint64
}

// NewReplicaPromotionBarrier creates a bounded local promotion barrier.
func NewReplicaPromotionBarrier(options ReplicaPromotionBarrierOptions) (*ReplicaPromotionBarrier, error) {
	maxReplicas := options.MaxReplicas
	if maxReplicas == 0 {
		maxReplicas = DefaultReplicaPromotionBarrierMaxReplicas
	}
	if maxReplicas < 1 || maxReplicas > maxReplicaPromotionBarrierReplicas {
		return nil, ErrReplicaPromotionBarrierOptionsInvalid
	}
	barrier := &ReplicaPromotionBarrier{maxReplicas: maxReplicas}
	barrier.state.Store(&replicaPromotionBarrierState{replicas: make(map[string]uint64)})
	return barrier, nil
}

// ObserveSource publishes a monotone source journal sequence. Equal values
// are idempotent and lower values are rejected.
func (barrier *ReplicaPromotionBarrier) ObserveSource(sequence uint64) error {
	if barrier == nil {
		return ErrReplicaPromotionBarrierNil
	}
	barrier.updateMu.Lock()
	defer barrier.updateMu.Unlock()
	current := barrier.state.Load()
	if sequence < current.sourceSequence {
		return ErrReplicaPromotionBarrierSequenceRegression
	}
	if sequence == current.sourceSequence {
		return nil
	}
	next := *current
	next.sourceSequence = sequence
	barrier.state.Store(&next)
	return nil
}

// ObserveReplica publishes a monotone applied sequence for node. Adding a new
// node consumes one bounded replica slot; existing nodes can be updated in
// place through a new immutable state.
func (barrier *ReplicaPromotionBarrier) ObserveReplica(node string, sequence uint64) error {
	if barrier == nil {
		return ErrReplicaPromotionBarrierNil
	}
	node, err := normalizeReplicaPromotionNode(node)
	if err != nil {
		return err
	}
	barrier.updateMu.Lock()
	defer barrier.updateMu.Unlock()
	current := barrier.state.Load()
	previous, exists := current.replicas[node]
	if exists {
		if sequence < previous {
			return ErrReplicaPromotionBarrierSequenceRegression
		}
		if sequence == previous {
			return nil
		}
	} else if len(current.replicas) >= barrier.maxReplicas {
		return ErrReplicaPromotionBarrierOptionsInvalid
	}
	replicas := make(map[string]uint64, len(current.replicas)+1)
	for currentNode, currentSequence := range current.replicas {
		replicas[currentNode] = currentSequence
	}
	replicas[node] = sequence
	next := *current
	next.replicas = replicas
	barrier.state.Store(&next)
	return nil
}

// Capture returns a promotion token only when node has reached the current
// source sequence. Capture itself does not advance the generation.
func (barrier *ReplicaPromotionBarrier) Capture(node string) (ReplicaPromotionToken, error) {
	if barrier == nil {
		return ReplicaPromotionToken{}, ErrReplicaPromotionBarrierNil
	}
	node, err := normalizeReplicaPromotionNode(node)
	if err != nil {
		return ReplicaPromotionToken{}, err
	}
	state := barrier.state.Load()
	applied, exists := state.replicas[node]
	if !exists || applied < state.sourceSequence {
		return ReplicaPromotionToken{}, ErrReplicaPromotionBarrierNotCaughtUp
	}
	return ReplicaPromotionToken{
		Generation:       barrier.generation.Load(),
		Node:             node,
		RequiredSequence: state.sourceSequence,
		AppliedSequence:  applied,
	}, nil
}

// Promote atomically accepts a captured token and advances the generation.
// The source sequence must not change between Capture and Promote; callers
// should fence or stop the old primary before Capture.
func (barrier *ReplicaPromotionBarrier) Promote(token ReplicaPromotionToken) (ReplicaPromotionResult, error) {
	if barrier == nil {
		return ReplicaPromotionResult{}, ErrReplicaPromotionBarrierNil
	}
	if token.Node == "" || strings.TrimSpace(token.Node) != token.Node {
		return ReplicaPromotionResult{}, ErrReplicaPromotionBarrierInvalidNode
	}
	barrier.updateMu.Lock()
	defer barrier.updateMu.Unlock()
	current := barrier.state.Load()
	currentGeneration := barrier.generation.Load()
	if token.Generation != currentGeneration {
		return ReplicaPromotionResult{}, ErrReplicaPromotionBarrierStaleToken
	}
	if token.RequiredSequence != current.sourceSequence {
		return ReplicaPromotionResult{}, ErrReplicaPromotionBarrierSourceAdvanced
	}
	applied, exists := current.replicas[token.Node]
	if !exists || applied < current.sourceSequence {
		return ReplicaPromotionResult{}, ErrReplicaPromotionBarrierNotCaughtUp
	}
	if applied < token.AppliedSequence {
		return ReplicaPromotionResult{}, ErrReplicaPromotionBarrierStaleToken
	}
	if currentGeneration == ^uint64(0) {
		return ReplicaPromotionResult{}, ErrReplicaPromotionBarrierGenerationExhausted
	}
	nextGeneration := currentGeneration + 1
	barrier.generation.Store(nextGeneration)
	return ReplicaPromotionResult{
		Generation:      nextGeneration,
		Node:            token.Node,
		SourceSequence:  current.sourceSequence,
		AppliedSequence: applied,
	}, nil
}

// Snapshot returns a detached, node-sorted progress view.
func (barrier *ReplicaPromotionBarrier) Snapshot() ReplicaPromotionBarrierSnapshot {
	if barrier == nil {
		return ReplicaPromotionBarrierSnapshot{}
	}
	state := barrier.state.Load()
	snapshot := ReplicaPromotionBarrierSnapshot{
		Generation:     barrier.generation.Load(),
		SourceSequence: state.sourceSequence,
		Replicas:       make([]ReplicaPromotionProgress, 0, len(state.replicas)),
	}
	for node, sequence := range state.replicas {
		snapshot.Replicas = append(snapshot.Replicas, ReplicaPromotionProgress{Node: node, AppliedSequence: sequence})
	}
	sort.Slice(snapshot.Replicas, func(left, right int) bool {
		return snapshot.Replicas[left].Node < snapshot.Replicas[right].Node
	})
	return snapshot
}

func normalizeReplicaPromotionNode(node string) (string, error) {
	node = strings.TrimSpace(node)
	if node == "" {
		return "", ErrReplicaPromotionBarrierInvalidNode
	}
	return node, nil
}
