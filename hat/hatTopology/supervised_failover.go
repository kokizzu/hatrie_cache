package hatTopology

import (
	"errors"
	"sort"
	"strings"
	"time"
)

// ElectionRecoveryState describes the operator-controlled state of a shard.
type ElectionRecoveryState string

const (
	// ElectionRecoveryAutomatic means normal topology and liveness election.
	ElectionRecoveryAutomatic ElectionRecoveryState = "automatic"
	// ElectionRecoveryOperatorOverride means an operator-selected leader is
	// pinned until the operator begins recovery.
	ElectionRecoveryOperatorOverride ElectionRecoveryState = "operator_override"
	// ElectionRecoveryInProgress keeps the selected leader pinned while the
	// operator verifies recovery before returning to automatic election.
	ElectionRecoveryInProgress ElectionRecoveryState = "recovery"
)

// ElectionControl describes an explicit operator decision for one shard.
// Controls are in-memory state and should be persisted by the control plane
// that owns failover decisions.
type ElectionControl struct {
	Shard  uint32                `json:"shard"`
	State  ElectionRecoveryState `json:"state"`
	Leader string                `json:"leader"`
	Reason string                `json:"reason,omitempty"`
	Since  time.Time             `json:"since"`
}

type electionControl struct {
	state  ElectionRecoveryState
	leader string
	reason string
	since  time.Time
}

// SetLeaderOverride pins a healthy shard owner as the supervised leader.
// Repeating the operation is an explicit replacement of the previous
// operator decision and returns the shard to operator-override state.
func (store *ElectionStore) SetLeaderOverride(shardID uint32, nodeID, reason string) error {
	if store == nil {
		return errors.New("hatriecache: election store is nil")
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return errors.New("hatriecache: supervised leader is required")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return errors.New("hatriecache: supervised override reason is required")
	}
	topology, ok := store.topologySnapshot()
	if !ok {
		return errors.New("hatriecache: election topology is unavailable")
	}
	shard, ok := electionShard(topology, shardID)
	if !ok {
		return errors.New("hatriecache: supervised shard is not registered")
	}
	if !electionShardOwnsNode(shard, nodeID) {
		return errors.New("hatriecache: supervised node is not an owner")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.now()
	if !store.nodeActiveLocked(topology.Nodes, nodeID, now) {
		return errors.New("hatriecache: supervised node is not healthy")
	}
	if store.controls == nil {
		store.controls = make(map[uint32]electionControl)
	}
	store.controls[shardID] = electionControl{
		state:  ElectionRecoveryOperatorOverride,
		leader: nodeID,
		reason: reason,
		since:  now,
	}
	return nil
}

// BeginRecovery changes an operator override to an explicit recovery phase.
// Automatic election remains disabled until CompleteRecovery succeeds.
func (store *ElectionStore) BeginRecovery(shardID uint32, reason string) error {
	if store == nil {
		return errors.New("hatriecache: election store is nil")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return errors.New("hatriecache: recovery reason is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	control, ok := store.controls[shardID]
	if !ok || control.state != ElectionRecoveryOperatorOverride {
		return errors.New("hatriecache: leader override is required before recovery")
	}
	control.state = ElectionRecoveryInProgress
	control.reason = reason
	control.since = store.now()
	store.controls[shardID] = control
	return nil
}

// CompleteRecovery releases a supervised shard only when its controlled
// leader is healthy. Automatic topology election resumes afterward.
func (store *ElectionStore) CompleteRecovery(shardID uint32) error {
	if store == nil {
		return errors.New("hatriecache: election store is nil")
	}
	topology, ok := store.topologySnapshot()
	if !ok {
		return errors.New("hatriecache: election topology is unavailable")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	control, ok := store.controls[shardID]
	if !ok || control.state != ElectionRecoveryInProgress {
		return errors.New("hatriecache: recovery is not in progress")
	}
	if !store.nodeActiveLocked(topology.Nodes, control.leader, store.now()) {
		return errors.New("hatriecache: controlled leader is not healthy")
	}
	delete(store.controls, shardID)
	if len(store.controls) == 0 {
		store.controls = nil
	}
	return nil
}

// Controls returns a sorted snapshot of all supervised shard decisions.
func (store *ElectionStore) Controls() []ElectionControl {
	if store == nil {
		return nil
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.controlsSnapshotLocked()
}

func (store *ElectionStore) controlsSnapshotLocked() []ElectionControl {
	if len(store.controls) == 0 {
		return nil
	}
	controls := make([]ElectionControl, 0, len(store.controls))
	for shardID, control := range store.controls {
		controls = append(controls, ElectionControl{
			Shard:  shardID,
			State:  control.state,
			Leader: control.leader,
			Reason: control.reason,
			Since:  control.since,
		})
	}
	sort.Slice(controls, func(i, j int) bool { return controls[i].Shard < controls[j].Shard })
	return controls
}

func (store *ElectionStore) controlledLeaderLocked(shard TopologyShard, nodes []TopologyNode, now time.Time) (ElectionLeader, bool) {
	if store.controls == nil {
		return ElectionLeader{}, false
	}
	control, ok := store.controls[shard.ID]
	if !ok {
		return ElectionLeader{}, false
	}
	return ElectionLeader{
		Shard:      shard.ID,
		Primary:    shard.Primary,
		Candidates: Owners(shard),
		Leader:     control.leader,
		Available:  store.nodeActiveLocked(nodes, control.leader, now),
	}, true
}

func electionShard(topology ClusterTopology, shardID uint32) (TopologyShard, bool) {
	if ModeFor(topology) == TopologyModeFullReplica {
		shard, ok := topology.FullReplicaShard()
		return shard, ok && shard.ID == shardID
	}
	for _, shard := range topology.Shards {
		if shard.ID == shardID {
			return shard, true
		}
	}
	return TopologyShard{}, false
}

func electionShardOwnsNode(shard TopologyShard, nodeID string) bool {
	for _, owner := range Owners(shard) {
		if owner == nodeID {
			return true
		}
	}
	return false
}
