package hatReplication

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// ErrGlobalTimestampOracleInvalid reports invalid coordinator, request, or
	// snapshot state.
	ErrGlobalTimestampOracleInvalid = errors.New("hatriecache: global timestamp oracle input is invalid")
	// ErrGlobalTimestampOracleTermMismatch reports a request from an old or
	// otherwise inactive coordinator term.
	ErrGlobalTimestampOracleTermMismatch = errors.New("hatriecache: global timestamp oracle term mismatch")
	// ErrGlobalTimestampOracleStaleTerm reports an attempt to move the
	// coordinator backwards.
	ErrGlobalTimestampOracleStaleTerm = errors.New("hatriecache: global timestamp oracle term is stale")
	// ErrGlobalTimestampOracleNodeEpochStale reports a request from an older
	// node process.
	ErrGlobalTimestampOracleNodeEpochStale = errors.New("hatriecache: global timestamp oracle node epoch is stale")
	// ErrGlobalTimestampOracleSequenceGap reports a missing per-node request.
	ErrGlobalTimestampOracleSequenceGap = errors.New("hatriecache: global timestamp oracle request sequence has a gap")
	// ErrGlobalTimestampOracleSequenceStale reports an already superseded
	// per-node request.
	ErrGlobalTimestampOracleSequenceStale = errors.New("hatriecache: global timestamp oracle request sequence is stale")
	// ErrGlobalTimestampOracleRequestConflict reports a retry with changed
	// request parameters.
	ErrGlobalTimestampOracleRequestConflict = errors.New("hatriecache: global timestamp oracle request conflicts with its retry")
	// ErrGlobalTimestampOracleOverflow reports that the timestamp range cannot
	// fit in the signed timestamp domain.
	ErrGlobalTimestampOracleOverflow = errors.New("hatriecache: global timestamp oracle overflow")
)

// GlobalTimestampRequest asks the consensus-selected timestamp coordinator to
// reserve Count consecutive timestamps for one node process. Term identifies
// the active coordinator term; NodeEpoch must come from a restart-fenced node
// epoch; and Sequence makes retries idempotent without retaining every
// historical request.
type GlobalTimestampRequest struct {
	Term      uint64 `json:"term"`
	NodeID    string `json:"node_id"`
	NodeEpoch uint64 `json:"node_epoch"`
	Sequence  uint64 `json:"sequence"`
	Observed  int64  `json:"observed"`
	Count     uint64 `json:"count"`
}

// GlobalTimestampGrant is a contiguous, globally unique timestamp range
// returned by a coordinator. Grants from one oracle never overlap and are
// ordered by Start. Unused values after a node failure remain gaps, which is
// preferable to reusing a timestamp that may already have been published.
type GlobalTimestampGrant struct {
	Term      uint64 `json:"term"`
	NodeID    string `json:"node_id"`
	NodeEpoch uint64 `json:"node_epoch"`
	Sequence  uint64 `json:"sequence"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
	Count     uint64 `json:"count"`
}

// GlobalTimestampNodeSnapshot is the bounded retry state retained for one
// node. The coordinator stores one record per current node epoch, rather than
// an unbounded request-id history.
type GlobalTimestampNodeSnapshot struct {
	NodeID    string               `json:"node_id"`
	NodeEpoch uint64               `json:"node_epoch"`
	Sequence  uint64               `json:"sequence"`
	Observed  int64                `json:"observed"`
	Grant     GlobalTimestampGrant `json:"grant"`
}

// GlobalTimestampOracleSnapshot is a deterministic, portable state-machine
// snapshot. A consensus log or durable store should publish this snapshot
// atomically with the coordinator state it protects.
type GlobalTimestampOracleSnapshot struct {
	Term    uint64                        `json:"term"`
	Current int64                         `json:"current"`
	Nodes   []GlobalTimestampNodeSnapshot `json:"nodes,omitempty"`
}

type globalTimestampNodeState struct {
	observed int64
	grant    GlobalTimestampGrant
}

// GlobalTimestampOracle is the serialized state machine behind a global
// timestamp authority. It is intentionally transport-neutral: callers must
// place one instance behind their consensus or single-writer control plane.
// It does not elect a leader, replicate state, or make independent processes
// share memory by itself.
type GlobalTimestampOracle struct {
	mu      sync.Mutex
	term    uint64
	current int64
	nodes   map[string]globalTimestampNodeState
}

// NewGlobalTimestampOracle creates a coordinator at term with a nonnegative
// starting timestamp. Terms start at one so stale coordinator instances can
// be fenced explicitly.
func NewGlobalTimestampOracle(term uint64, initial int64) (*GlobalTimestampOracle, error) {
	if term == 0 || initial < 0 {
		return nil, ErrGlobalTimestampOracleInvalid
	}
	return &GlobalTimestampOracle{
		term:    term,
		current: initial,
		nodes:   make(map[string]globalTimestampNodeState),
	}, nil
}

// NewGlobalTimestampOracleFromSnapshot restores a validated coordinator
// snapshot. Node entries are copied and normalized into deterministic map
// state; callers can then atomically publish the restored instance.
func NewGlobalTimestampOracleFromSnapshot(snapshot GlobalTimestampOracleSnapshot) (*GlobalTimestampOracle, error) {
	if snapshot.Term == 0 || snapshot.Current < 0 {
		return nil, ErrGlobalTimestampOracleInvalid
	}
	oracle := &GlobalTimestampOracle{
		term:    snapshot.Term,
		current: snapshot.Current,
		nodes:   make(map[string]globalTimestampNodeState, len(snapshot.Nodes)),
	}
	ranges := make([]GlobalTimestampGrant, 0, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodeID := strings.TrimSpace(node.NodeID)
		if nodeID == "" || node.NodeEpoch == 0 || node.Sequence == 0 || node.Observed < 0 {
			return nil, ErrGlobalTimestampOracleInvalid
		}
		if _, exists := oracle.nodes[nodeID]; exists {
			return nil, fmt.Errorf("%w: duplicate node %q", ErrGlobalTimestampOracleInvalid, nodeID)
		}
		grant := node.Grant
		if grant.NodeID != nodeID || grant.NodeEpoch != node.NodeEpoch || grant.Sequence != node.Sequence || grant.Term > snapshot.Term || grant.End > snapshot.Current {
			return nil, ErrGlobalTimestampOracleInvalid
		}
		if err := validateGlobalTimestampGrant(grant); err != nil {
			return nil, err
		}
		if node.Observed >= grant.Start || node.Observed > snapshot.Current {
			return nil, ErrGlobalTimestampOracleInvalid
		}
		oracle.nodes[nodeID] = globalTimestampNodeState{observed: node.Observed, grant: grant}
		ranges = append(ranges, grant)
	}
	sort.Slice(ranges, func(left, right int) bool { return ranges[left].Start < ranges[right].Start })
	for index := 1; index < len(ranges); index++ {
		if ranges[index].Start <= ranges[index-1].End {
			return nil, ErrGlobalTimestampOracleInvalid
		}
	}
	return oracle, nil
}

// Current returns the greatest timestamp allocated or observed by the
// coordinator. A nil oracle reports zero.
func (oracle *GlobalTimestampOracle) Current() int64 {
	if oracle == nil {
		return 0
	}
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	return oracle.current
}

// Term returns the active coordinator term. A nil oracle reports zero.
func (oracle *GlobalTimestampOracle) Term() uint64 {
	if oracle == nil {
		return 0
	}
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	return oracle.term
}

// AdvanceTerm fences older coordinators. Equal terms are accepted as an
// idempotent retry; lower terms are rejected and do not change timestamp
// state.
func (oracle *GlobalTimestampOracle) AdvanceTerm(term uint64) error {
	if oracle == nil || term == 0 {
		return ErrGlobalTimestampOracleInvalid
	}
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	if term < oracle.term {
		return ErrGlobalTimestampOracleStaleTerm
	}
	oracle.term = term
	return nil
}

// Reserve atomically observes a caller timestamp and allocates a contiguous
// range. A repeated request with the same node epoch, sequence, observed
// value, and count returns the original grant without advancing the clock.
func (oracle *GlobalTimestampOracle) Reserve(request GlobalTimestampRequest) (GlobalTimestampGrant, error) {
	if oracle == nil {
		return GlobalTimestampGrant{}, ErrGlobalTimestampOracleInvalid
	}
	request.NodeID = strings.TrimSpace(request.NodeID)
	if request.Term == 0 || request.NodeID == "" || request.NodeEpoch == 0 || request.Sequence == 0 || request.Count == 0 || request.Observed < 0 {
		return GlobalTimestampGrant{}, ErrGlobalTimestampOracleInvalid
	}

	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	if request.Term != oracle.term {
		return GlobalTimestampGrant{}, ErrGlobalTimestampOracleTermMismatch
	}
	state, exists := oracle.nodes[request.NodeID]
	if exists {
		last := state.grant
		switch {
		case request.NodeEpoch < last.NodeEpoch:
			return GlobalTimestampGrant{}, ErrGlobalTimestampOracleNodeEpochStale
		case request.NodeEpoch > last.NodeEpoch:
			if request.Sequence != 1 {
				return GlobalTimestampGrant{}, ErrGlobalTimestampOracleSequenceGap
			}
		case request.Sequence == last.Sequence:
			if request.Term != last.Term {
				return GlobalTimestampGrant{}, ErrGlobalTimestampOracleTermMismatch
			}
			if request.NodeEpoch != last.NodeEpoch || request.Count != last.Count || request.Observed != state.observed {
				return GlobalTimestampGrant{}, ErrGlobalTimestampOracleRequestConflict
			}
			return last, nil
		case request.Sequence < last.Sequence:
			return GlobalTimestampGrant{}, ErrGlobalTimestampOracleSequenceStale
		case last.Sequence == ^uint64(0) || request.Sequence != last.Sequence+1:
			return GlobalTimestampGrant{}, ErrGlobalTimestampOracleSequenceGap
		}
	} else if request.Sequence != 1 {
		return GlobalTimestampGrant{}, ErrGlobalTimestampOracleSequenceGap
	}

	current := oracle.current
	if request.Observed > current {
		current = request.Observed
	}
	if request.Count > uint64(timestampOracleMax-current) {
		return GlobalTimestampGrant{}, ErrGlobalTimestampOracleOverflow
	}
	start := current + 1
	end := current + int64(request.Count)
	grant := GlobalTimestampGrant{
		Term:      oracle.term,
		NodeID:    request.NodeID,
		NodeEpoch: request.NodeEpoch,
		Sequence:  request.Sequence,
		Start:     start,
		End:       end,
		Count:     request.Count,
	}
	oracle.current = end
	if oracle.nodes == nil {
		oracle.nodes = make(map[string]globalTimestampNodeState)
	}
	oracle.nodes[request.NodeID] = globalTimestampNodeState{observed: request.Observed, grant: grant}
	return grant, nil
}

// Snapshot returns a deterministic copy of the coordinator state. The node
// list is sorted by NodeID so equivalent states have identical serialized
// order.
func (oracle *GlobalTimestampOracle) Snapshot() GlobalTimestampOracleSnapshot {
	if oracle == nil {
		return GlobalTimestampOracleSnapshot{}
	}
	oracle.mu.Lock()
	defer oracle.mu.Unlock()
	snapshot := GlobalTimestampOracleSnapshot{
		Term:    oracle.term,
		Current: oracle.current,
		Nodes:   make([]GlobalTimestampNodeSnapshot, 0, len(oracle.nodes)),
	}
	for nodeID, state := range oracle.nodes {
		snapshot.Nodes = append(snapshot.Nodes, GlobalTimestampNodeSnapshot{
			NodeID:    nodeID,
			NodeEpoch: state.grant.NodeEpoch,
			Sequence:  state.grant.Sequence,
			Observed:  state.observed,
			Grant:     state.grant,
		})
	}
	sort.Slice(snapshot.Nodes, func(left, right int) bool { return snapshot.Nodes[left].NodeID < snapshot.Nodes[right].NodeID })
	return snapshot
}

// GlobalTimestampLease consumes one grant locally. It is safe for concurrent
// callers and performs no allocations after construction. Reset must only be
// called after the previous lease is exhausted and all callers have stopped
// using it.
type GlobalTimestampLease struct {
	next atomic.Uint64
	end  uint64
}

// NewGlobalTimestampLease creates a local consumer for one valid grant.
func NewGlobalTimestampLease(grant GlobalTimestampGrant) (*GlobalTimestampLease, error) {
	if err := validateGlobalTimestampGrant(grant); err != nil {
		return nil, err
	}
	lease := &GlobalTimestampLease{end: uint64(grant.End)}
	lease.next.Store(uint64(grant.Start))
	return lease, nil
}

// Lease creates a local consumer for this grant.
func (grant GlobalTimestampGrant) Lease() (*GlobalTimestampLease, error) {
	return NewGlobalTimestampLease(grant)
}

// Reset replaces an exhausted lease without allocating a new consumer.
func (lease *GlobalTimestampLease) Reset(grant GlobalTimestampGrant) error {
	if lease == nil {
		return ErrGlobalTimestampOracleInvalid
	}
	if err := validateGlobalTimestampGrant(grant); err != nil {
		return err
	}
	lease.end = uint64(grant.End)
	lease.next.Store(uint64(grant.Start))
	return nil
}

// Next returns one timestamp, or false after the range is exhausted.
func (lease *GlobalTimestampLease) Next() (int64, bool) {
	if lease == nil {
		return 0, false
	}
	for {
		current := lease.next.Load()
		if current == 0 || current > lease.end {
			return 0, false
		}
		if current == lease.end {
			if lease.next.CompareAndSwap(current, 0) {
				return int64(current), true
			}
			continue
		}
		if lease.next.CompareAndSwap(current, current+1) {
			return int64(current), true
		}
	}
}

// Remaining returns the number of timestamps not yet consumed.
func (lease *GlobalTimestampLease) Remaining() uint64 {
	if lease == nil {
		return 0
	}
	current := lease.next.Load()
	if current == 0 || current > lease.end {
		return 0
	}
	return lease.end - current + 1
}

func validateGlobalTimestampGrant(grant GlobalTimestampGrant) error {
	if grant.Term == 0 || strings.TrimSpace(grant.NodeID) == "" || grant.NodeEpoch == 0 || grant.Sequence == 0 || grant.Count == 0 || grant.Start <= 0 || grant.End < grant.Start {
		return ErrGlobalTimestampOracleInvalid
	}
	if uint64(grant.End-grant.Start)+1 != grant.Count {
		return ErrGlobalTimestampOracleInvalid
	}
	return nil
}
