package hatReplication

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultReplicaRecoveryMaxNodes bounds the in-memory recovery tombstone
	// registry when callers do not provide a limit.
	DefaultReplicaRecoveryMaxNodes = 1024
	// MaxReplicaRecoveryMaxNodes prevents an accidental unbounded registry.
	MaxReplicaRecoveryMaxNodes = 65536
)

var (
	// ErrReplicaRecoveryNil indicates a nil recovery protocol.
	ErrReplicaRecoveryNil = errors.New("hatReplication: replica recovery protocol is nil")
	// ErrReplicaRecoveryInvalid indicates malformed recovery metadata.
	ErrReplicaRecoveryInvalid = errors.New("hatReplication: replica recovery metadata is invalid")
	// ErrReplicaRecoveryLimit indicates that the bounded registry is full.
	ErrReplicaRecoveryLimit = errors.New("hatReplication: replica recovery registry limit reached")
	// ErrReplicaRecoveryGeneration indicates a stale recovery plan generation.
	ErrReplicaRecoveryGeneration = errors.New("hatReplication: replica recovery generation mismatch")
	// ErrReplicaRecoveryFencing indicates a stale recovery fencing token.
	ErrReplicaRecoveryFencing = errors.New("hatReplication: replica recovery fencing token is stale")
	// ErrReplicaRecoveryNodeExists indicates a duplicate active admission.
	ErrReplicaRecoveryNodeExists = errors.New("hatReplication: replica recovery node already exists")
	// ErrReplicaRecoveryNodeMissing indicates an eviction of an unknown node.
	ErrReplicaRecoveryNodeMissing = errors.New("hatReplication: replica recovery node does not exist")
	// ErrReplicaRecoveryAlreadyEvicted indicates a repeated eviction.
	ErrReplicaRecoveryAlreadyEvicted = errors.New("hatReplication: replica recovery node is already evicted")
	// ErrReplicaRecoveryStaleIncarnation indicates an old process incarnation.
	ErrReplicaRecoveryStaleIncarnation = errors.New("hatReplication: replica recovery incarnation is stale")
	// ErrReplicaRecoveryEvictionRequired indicates that a live node changed
	// incarnation without an explicit eviction.
	ErrReplicaRecoveryEvictionRequired = errors.New("hatReplication: replica recovery requires eviction before incarnation change")
	// ErrReplicaRecoveryStaleState indicates that a node presented older state
	// than the registry has already committed for its incarnation.
	ErrReplicaRecoveryStaleState = errors.New("hatReplication: replica recovery state is stale")
	// ErrReplicaRecoveryFutureState indicates state beyond the advertised source
	// frontier or storage generation.
	ErrReplicaRecoveryFutureState = errors.New("hatReplication: replica recovery state is from the future")
	// ErrReplicaRecoverySequence indicates insufficient rejoin progress.
	ErrReplicaRecoverySequence = errors.New("hatReplication: replica recovery sequence is insufficient")
)

// ReplicaRecoveryState describes the retained lifecycle state of one node.
type ReplicaRecoveryState uint8

const (
	ReplicaRecoveryStateUnknown ReplicaRecoveryState = iota
	ReplicaRecoveryStateActive
	ReplicaRecoveryStateEvicted
)

// String returns a stable operator-facing state name.
func (state ReplicaRecoveryState) String() string {
	switch state {
	case ReplicaRecoveryStateUnknown:
		return "unknown"
	case ReplicaRecoveryStateActive:
		return "active"
	case ReplicaRecoveryStateEvicted:
		return "evicted"
	default:
		return "invalid"
	}
}

// ReplicaRecoveryDecision is the safe action for a rejoining node.
type ReplicaRecoveryDecision uint8

const (
	ReplicaRecoveryDecisionReject ReplicaRecoveryDecision = iota
	ReplicaRecoveryDecisionResume
	ReplicaRecoveryDecisionBootstrap
)

// String returns a stable decision name.
func (decision ReplicaRecoveryDecision) String() string {
	switch decision {
	case ReplicaRecoveryDecisionReject:
		return "reject"
	case ReplicaRecoveryDecisionResume:
		return "resume"
	case ReplicaRecoveryDecisionBootstrap:
		return "bootstrap"
	default:
		return "invalid"
	}
}

// ReplicaRecoveryOptions bounds the retained node and eviction tombstone
// registry.
type ReplicaRecoveryOptions struct {
	MaxNodes int
}

// ReplicaRecoveryNode is the committed lifecycle and progress state for a
// node. Evicted nodes remain as tombstones so an old process cannot rejoin.
type ReplicaRecoveryNode struct {
	NodeID              string
	Incarnation         uint64
	State               ReplicaRecoveryState
	LastAppliedSequence uint64
	StorageGeneration   uint64
	EvictedAtGeneration uint64
}

// ReplicaRecoverySource describes the source frontier available to a rejoin.
// RetainedFromSequence is inclusive and identifies the earliest replayable WAL
// sequence; a node below it must receive a fresh snapshot.
type ReplicaRecoverySource struct {
	CurrentJournalSequence uint64
	RetainedFromSequence   uint64
	StorageGeneration      uint64
}

// ReplicaRejoinRequest carries the node's self-reported incarnation and local
// state. It is advisory until CommitRejoin succeeds.
type ReplicaRejoinRequest struct {
	NodeID              string
	Incarnation         uint64
	LastAppliedSequence uint64
	StorageGeneration   uint64
}

// ReplicaRejoinPlan is a generation-fenced, side-effect-free recovery decision.
// Callers perform the actual resume or snapshot/WAL bootstrap, then pass the
// plan to CommitRejoin with the resulting applied sequence.
type ReplicaRejoinPlan struct {
	NodeID                  string
	Incarnation             uint64
	Decision                ReplicaRecoveryDecision
	LastAppliedSequence     uint64
	StorageGeneration       uint64
	SourceCurrentSequence   uint64
	SourceRetainedFrom      uint64
	SourceStorageGeneration uint64
	RequiredSequence        uint64
	ExpectedGeneration      uint64
	FencingToken            uint64
	Reason                  string
}

// ReplicaRecoverySnapshot is a detached, deterministically ordered view of
// the recovery registry.
type ReplicaRecoverySnapshot struct {
	Generation   uint64
	FencingToken uint64
	Nodes        []ReplicaRecoveryNode
}

// ReplicaRecoveryProtocol owns recovery tombstones and generation/fencing
// checks. It does not mutate topology, transfer snapshots, or open transports.
type ReplicaRecoveryProtocol struct {
	mu           sync.RWMutex
	maxNodes     int
	generation   uint64
	fencingToken uint64
	nodes        map[string]ReplicaRecoveryNode
}

// NewReplicaRecoveryProtocol creates an empty bounded recovery registry.
func NewReplicaRecoveryProtocol(options ReplicaRecoveryOptions) (*ReplicaRecoveryProtocol, error) {
	maxNodes := options.MaxNodes
	if maxNodes == 0 {
		maxNodes = DefaultReplicaRecoveryMaxNodes
	}
	if maxNodes < 1 || maxNodes > MaxReplicaRecoveryMaxNodes {
		return nil, ErrReplicaRecoveryInvalid
	}
	return &ReplicaRecoveryProtocol{
		maxNodes: maxNodes,
		nodes:    make(map[string]ReplicaRecoveryNode, maxNodes),
	}, nil
}

// Admit adds a new active node using an exact generation and newer fencing
// token. Existing nodes must use EvaluateRejoin and CommitRejoin so evicted
// identities cannot be silently replaced.
func (protocol *ReplicaRecoveryProtocol) Admit(node ReplicaRecoveryNode, expectedGeneration, fencingToken uint64) (ReplicaRecoverySnapshot, error) {
	if protocol == nil {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryNil
	}
	normalized, err := normalizeReplicaRecoveryNode(node)
	if err != nil {
		return ReplicaRecoverySnapshot{}, err
	}
	protocol.mu.Lock()
	defer protocol.mu.Unlock()
	if err := protocol.validateMutationFenceLocked(expectedGeneration, fencingToken); err != nil {
		return ReplicaRecoverySnapshot{}, err
	}
	if current, exists := protocol.nodes[normalized.NodeID]; exists {
		if current.State == ReplicaRecoveryStateEvicted && normalized.Incarnation <= current.Incarnation {
			return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryStaleIncarnation
		}
		if current.State == ReplicaRecoveryStateEvicted {
			return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryEvictionRequired
		}
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryNodeExists
	}
	if len(protocol.nodes) >= protocol.maxNodes {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryLimit
	}
	normalized.State = ReplicaRecoveryStateActive
	protocol.nodes[normalized.NodeID] = normalized
	protocol.generation++
	protocol.fencingToken = fencingToken
	return protocol.snapshotLocked(), nil
}

// Evict marks a node unavailable while retaining its incarnation tombstone.
// A later rejoin must present a strictly newer incarnation.
func (protocol *ReplicaRecoveryProtocol) Evict(nodeID string, expectedGeneration, fencingToken uint64) (ReplicaRecoverySnapshot, error) {
	if protocol == nil {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryNil
	}
	normalizedID, err := normalizeSnapshotWALBootstrapIdentifier(nodeID, "node ID")
	if err != nil {
		return ReplicaRecoverySnapshot{}, fmt.Errorf("%w: %v", ErrReplicaRecoveryInvalid, err)
	}
	protocol.mu.Lock()
	defer protocol.mu.Unlock()
	if err := protocol.validateMutationFenceLocked(expectedGeneration, fencingToken); err != nil {
		return ReplicaRecoverySnapshot{}, err
	}
	node, exists := protocol.nodes[normalizedID]
	if !exists {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryNodeMissing
	}
	if node.State == ReplicaRecoveryStateEvicted {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryAlreadyEvicted
	}
	node.State = ReplicaRecoveryStateEvicted
	protocol.generation++
	node.EvictedAtGeneration = protocol.generation
	protocol.fencingToken = fencingToken
	protocol.nodes[normalizedID] = node
	return protocol.snapshotLocked(), nil
}

// EvaluateRejoin validates a node's self-reported state against one source
// frontier without changing protocol state.
func (protocol *ReplicaRecoveryProtocol) EvaluateRejoin(request ReplicaRejoinRequest, source ReplicaRecoverySource) (ReplicaRejoinPlan, error) {
	if protocol == nil {
		return ReplicaRejoinPlan{}, ErrReplicaRecoveryNil
	}
	normalized, err := normalizeReplicaRejoinRequest(request)
	if err != nil {
		return ReplicaRejoinPlan{}, err
	}
	if source.StorageGeneration == 0 || source.RetainedFromSequence > source.CurrentJournalSequence {
		return ReplicaRejoinPlan{}, fmt.Errorf("%w: invalid source frontier", ErrReplicaRecoveryInvalid)
	}
	if normalized.LastAppliedSequence > source.CurrentJournalSequence || normalized.StorageGeneration > source.StorageGeneration {
		return ReplicaRejoinPlan{}, ErrReplicaRecoveryFutureState
	}

	protocol.mu.RLock()
	defer protocol.mu.RUnlock()
	current, exists := protocol.nodes[normalized.NodeID]
	if exists {
		if normalized.Incarnation < current.Incarnation {
			return ReplicaRejoinPlan{}, ErrReplicaRecoveryStaleIncarnation
		}
		if current.State == ReplicaRecoveryStateActive && normalized.Incarnation > current.Incarnation {
			return ReplicaRejoinPlan{}, ErrReplicaRecoveryEvictionRequired
		}
		if current.State == ReplicaRecoveryStateEvicted && normalized.Incarnation <= current.Incarnation {
			return ReplicaRejoinPlan{}, ErrReplicaRecoveryStaleIncarnation
		}
		if current.State == ReplicaRecoveryStateActive && normalized.LastAppliedSequence < current.LastAppliedSequence {
			return ReplicaRejoinPlan{}, ErrReplicaRecoveryStaleState
		}
	}

	decision := ReplicaRecoveryDecisionResume
	reason := "replayable WAL and compatible storage generation"
	if !exists {
		decision = ReplicaRecoveryDecisionBootstrap
		reason = "unknown node requires a complete bootstrap"
	} else if normalized.StorageGeneration != source.StorageGeneration {
		decision = ReplicaRecoveryDecisionBootstrap
		reason = "storage generation differs from source"
	} else if normalized.LastAppliedSequence < source.RetainedFromSequence {
		decision = ReplicaRecoveryDecisionBootstrap
		reason = "node state predates retained WAL"
	}
	requiredSequence := normalized.LastAppliedSequence
	if decision == ReplicaRecoveryDecisionBootstrap {
		requiredSequence = source.CurrentJournalSequence
	}
	return ReplicaRejoinPlan{
		NodeID:                  normalized.NodeID,
		Incarnation:             normalized.Incarnation,
		Decision:                decision,
		LastAppliedSequence:     normalized.LastAppliedSequence,
		StorageGeneration:       normalized.StorageGeneration,
		SourceCurrentSequence:   source.CurrentJournalSequence,
		SourceRetainedFrom:      source.RetainedFromSequence,
		SourceStorageGeneration: source.StorageGeneration,
		RequiredSequence:        requiredSequence,
		ExpectedGeneration:      protocol.generation,
		FencingToken:            protocol.fencingToken,
		Reason:                  reason,
	}, nil
}

// CommitRejoin records successful recovery after the caller has completed the
// selected resume or bootstrap path. The plan generation and fencing token
// must still be current, and the new fencing token must be strictly greater.
func (protocol *ReplicaRecoveryProtocol) CommitRejoin(plan ReplicaRejoinPlan, appliedThrough, storageGeneration, fencingToken uint64) (ReplicaRecoverySnapshot, error) {
	if protocol == nil {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryNil
	}
	nodeID, err := normalizeSnapshotWALBootstrapIdentifier(plan.NodeID, "node ID")
	if err != nil || nodeID != plan.NodeID || plan.Incarnation == 0 || plan.SourceStorageGeneration == 0 {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryInvalid
	}
	if plan.Decision != ReplicaRecoveryDecisionResume && plan.Decision != ReplicaRecoveryDecisionBootstrap {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryInvalid
	}
	if plan.SourceCurrentSequence < plan.RequiredSequence || appliedThrough < plan.RequiredSequence || appliedThrough > plan.SourceCurrentSequence {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoverySequence
	}
	if storageGeneration != plan.SourceStorageGeneration {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryInvalid
	}
	protocol.mu.Lock()
	defer protocol.mu.Unlock()
	if plan.ExpectedGeneration != protocol.generation {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryGeneration
	}
	if plan.FencingToken != protocol.fencingToken || fencingToken <= protocol.fencingToken {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryFencing
	}
	current, exists := protocol.nodes[plan.NodeID]
	if exists {
		if plan.Incarnation < current.Incarnation {
			return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryStaleIncarnation
		}
		if current.State == ReplicaRecoveryStateActive && plan.Incarnation > current.Incarnation {
			return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryEvictionRequired
		}
		if current.State == ReplicaRecoveryStateActive && appliedThrough < current.LastAppliedSequence {
			return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryStaleState
		}
	}
	if !exists && len(protocol.nodes) >= protocol.maxNodes {
		return ReplicaRecoverySnapshot{}, ErrReplicaRecoveryLimit
	}
	protocol.nodes[plan.NodeID] = ReplicaRecoveryNode{
		NodeID:              plan.NodeID,
		Incarnation:         plan.Incarnation,
		State:               ReplicaRecoveryStateActive,
		LastAppliedSequence: appliedThrough,
		StorageGeneration:   storageGeneration,
	}
	protocol.generation++
	protocol.fencingToken = fencingToken
	return protocol.snapshotLocked(), nil
}

// Snapshot returns a detached, sorted registry view.
func (protocol *ReplicaRecoveryProtocol) Snapshot() ReplicaRecoverySnapshot {
	if protocol == nil {
		return ReplicaRecoverySnapshot{}
	}
	protocol.mu.RLock()
	defer protocol.mu.RUnlock()
	return protocol.snapshotLocked()
}

func (protocol *ReplicaRecoveryProtocol) validateMutationFenceLocked(expectedGeneration, fencingToken uint64) error {
	if expectedGeneration != protocol.generation {
		return fmt.Errorf("%w: expected=%d current=%d", ErrReplicaRecoveryGeneration, expectedGeneration, protocol.generation)
	}
	if fencingToken <= protocol.fencingToken {
		return fmt.Errorf("%w: proposed=%d current=%d", ErrReplicaRecoveryFencing, fencingToken, protocol.fencingToken)
	}
	return nil
}

func (protocol *ReplicaRecoveryProtocol) snapshotLocked() ReplicaRecoverySnapshot {
	nodes := make([]ReplicaRecoveryNode, 0, len(protocol.nodes))
	for _, node := range protocol.nodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(left, right int) bool { return nodes[left].NodeID < nodes[right].NodeID })
	return ReplicaRecoverySnapshot{
		Generation:   protocol.generation,
		FencingToken: protocol.fencingToken,
		Nodes:        nodes,
	}
}

func normalizeReplicaRecoveryNode(node ReplicaRecoveryNode) (ReplicaRecoveryNode, error) {
	nodeID, err := normalizeSnapshotWALBootstrapIdentifier(node.NodeID, "node ID")
	if err != nil {
		return ReplicaRecoveryNode{}, fmt.Errorf("%w: %v", ErrReplicaRecoveryInvalid, err)
	}
	if node.Incarnation == 0 || node.StorageGeneration == 0 {
		return ReplicaRecoveryNode{}, ErrReplicaRecoveryInvalid
	}
	node.NodeID = nodeID
	node.State = ReplicaRecoveryStateActive
	node.EvictedAtGeneration = 0
	return node, nil
}

func normalizeReplicaRejoinRequest(request ReplicaRejoinRequest) (ReplicaRejoinRequest, error) {
	nodeID, err := normalizeSnapshotWALBootstrapIdentifier(request.NodeID, "node ID")
	if err != nil {
		return ReplicaRejoinRequest{}, fmt.Errorf("%w: %v", ErrReplicaRecoveryInvalid, err)
	}
	if request.Incarnation == 0 || request.StorageGeneration == 0 {
		return ReplicaRejoinRequest{}, ErrReplicaRecoveryInvalid
	}
	request.NodeID = strings.TrimSpace(nodeID)
	return request, nil
}
