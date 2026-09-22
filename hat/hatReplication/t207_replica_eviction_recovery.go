package hatReplication

import (
	"fmt"
	"reflect"
	"sort"
)

const (
	// MaxReplicaEvictionReasonBytes bounds operator-provided eviction context.
	MaxReplicaEvictionReasonBytes = 1024
)

// ReplicaEviction is a durable identity fence for a removed member. The epoch
// must be presented by a later rejoin; retaining the tombstone prevents a
// stale process from silently using the ordinary join path.
type ReplicaEviction struct {
	NodeID             string
	Address            string
	StorageGeneration  uint64
	MemberGeneration   uint64
	Role               ReplicaRole
	EvictionEpoch      uint64
	TopologyGeneration uint64
	Reason             string
}

// ReplicaRejoinRequest is a join request for an identity that was explicitly
// evicted. The embedded request carries the new bootstrap plan and may use a
// changed address; EvictionEpoch remains the stable stale-state fence.
type ReplicaRejoinRequest struct {
	ReplicaJoinRequest
	EvictionEpoch uint64
}

// ReplicaRejoinDecision is an immutable, generation-fenced rejoin admission.
type ReplicaRejoinDecision struct {
	ReplicaJoinDecision
	EvictionEpoch uint64
}

// ReplicaRecoverySnapshot is a detached observation of retained eviction
// fences and pending rejoin operations.
type ReplicaRecoverySnapshot struct {
	Generation     uint64
	Evictions      []ReplicaEviction
	PendingRejoins int
}

type replicaRejoinPending struct {
	request  ReplicaRejoinRequest
	decision ReplicaRejoinDecision
}

// Evict removes an active member and records a bounded stale-state fence. The
// expected generation is the member.Generation returned by the admission
// commit, so an old operator command cannot remove a newer incarnation.
func (admission *ReplicaJoinAdmission) Evict(nodeID string, expectedMemberGeneration uint64, reason string) (ReplicaEviction, error) {
	if admission == nil {
		return ReplicaEviction{}, ErrReplicaJoinAdmissionNil
	}
	if expectedMemberGeneration == 0 {
		return ReplicaEviction{}, ErrReplicaJoinAdmissionInvalid
	}
	normalizedNodeID, err := normalizeReplicaJoinValue(nodeID, MaxReplicaJoinNodeIDBytes, "node ID")
	if err != nil {
		return ReplicaEviction{}, err
	}
	normalizedReason, err := normalizeReplicaJoinValue(reason, MaxReplicaEvictionReasonBytes, "eviction reason")
	if err != nil {
		return ReplicaEviction{}, err
	}

	admission.mu.Lock()
	defer admission.mu.Unlock()
	member, exists := admission.members[normalizedNodeID]
	if !exists {
		return ReplicaEviction{}, ErrReplicaJoinAdmissionMemberNotFound
	}
	if member.Generation != expectedMemberGeneration {
		return ReplicaEviction{}, ErrReplicaJoinAdmissionGeneration
	}
	if len(admission.evicted) >= admission.maxEvictions {
		return ReplicaEviction{}, ErrReplicaJoinAdmissionEvictionLimit
	}
	if admission.evictionEpoch == ^uint64(0) || admission.generation == ^uint64(0) {
		return ReplicaEviction{}, ErrReplicaJoinAdmissionEvictionLimit
	}
	admission.evictionEpoch++
	admission.generation++
	eviction := ReplicaEviction{
		NodeID:             member.NodeID,
		Address:            member.Address,
		StorageGeneration:  member.StorageGeneration,
		MemberGeneration:   member.Generation,
		Role:               member.Role,
		EvictionEpoch:      admission.evictionEpoch,
		TopologyGeneration: admission.generation,
		Reason:             normalizedReason,
	}
	delete(admission.members, normalizedNodeID)
	admission.evicted[normalizedNodeID] = eviction
	return eviction, nil
}

// PrepareRejoin validates an evicted identity and deterministically selects a
// source. Exact retries return the original decision without changing state.
func (admission *ReplicaJoinAdmission) PrepareRejoin(request ReplicaRejoinRequest) (ReplicaRejoinDecision, error) {
	if admission == nil {
		return ReplicaRejoinDecision{}, ErrReplicaJoinAdmissionNil
	}
	if request.EvictionEpoch == 0 {
		return ReplicaRejoinDecision{}, ErrReplicaJoinAdmissionInvalid
	}
	normalized, err := normalizeReplicaJoinRequest(request.ReplicaJoinRequest, admission.maxCandidates)
	if err != nil {
		return ReplicaRejoinDecision{}, err
	}
	request.ReplicaJoinRequest = normalized

	admission.mu.Lock()
	defer admission.mu.Unlock()
	if _, exists := admission.members[normalized.JoinerID]; exists {
		return ReplicaRejoinDecision{}, ErrReplicaJoinAdmissionAlreadyPresent
	}
	eviction, exists := admission.evicted[normalized.JoinerID]
	if !exists {
		return ReplicaRejoinDecision{}, ErrReplicaJoinAdmissionNotEvicted
	}
	if request.EvictionEpoch != eviction.EvictionEpoch {
		return ReplicaRejoinDecision{}, ErrReplicaJoinAdmissionStaleEviction
	}
	if request.Role != eviction.Role {
		return ReplicaRejoinDecision{}, ErrReplicaJoinAdmissionRoleMismatch
	}
	if pending, exists := admission.rejoinPending[normalized.JoinerID]; exists {
		if reflect.DeepEqual(pending.request, request) {
			return pending.decision, nil
		}
		return ReplicaRejoinDecision{}, ErrReplicaJoinAdmissionConflict
	}
	if _, exists := admission.pending[normalized.JoinerID]; exists {
		return ReplicaRejoinDecision{}, ErrReplicaJoinAdmissionConflict
	}
	for nodeID, member := range admission.members {
		if member.Address == normalized.Address {
			return ReplicaRejoinDecision{}, fmt.Errorf("%w: active node %q", ErrReplicaJoinAdmissionAddressInUse, nodeID)
		}
	}
	for nodeID, pending := range admission.pending {
		if pending.request.Address == normalized.Address {
			return ReplicaRejoinDecision{}, fmt.Errorf("%w: pending node %q", ErrReplicaJoinAdmissionAddressInUse, nodeID)
		}
	}
	for nodeID, pending := range admission.rejoinPending {
		if pending.request.Address == normalized.Address {
			return ReplicaRejoinDecision{}, fmt.Errorf("%w: pending rejoin node %q", ErrReplicaJoinAdmissionAddressInUse, nodeID)
		}
	}
	for nodeID, otherEviction := range admission.evicted {
		if nodeID != normalized.JoinerID && otherEviction.Address == normalized.Address {
			return ReplicaRejoinDecision{}, fmt.Errorf("%w: evicted node %q", ErrReplicaJoinAdmissionAddressInUse, nodeID)
		}
	}
	if len(admission.members)+len(admission.pending)+len(admission.rejoinPending) >= admission.maxMembers {
		return ReplicaRejoinDecision{}, ErrReplicaJoinAdmissionLimit
	}
	source, err := selectReplicaJoinSource(normalized)
	if err != nil {
		return ReplicaRejoinDecision{}, err
	}
	decision := ReplicaRejoinDecision{
		ReplicaJoinDecision: ReplicaJoinDecision{
			JoinerID:           normalized.JoinerID,
			Address:            normalized.Address,
			SourceID:           source.NodeID,
			SourceAddress:      source.Address,
			Role:               normalized.Role,
			TopologyGeneration: admission.generation + 1,
			BootstrapPlan: SnapshotWALBootstrapPlan{
				JoinerID:                normalized.JoinerID,
				SourceID:                source.NodeID,
				SnapshotID:              normalized.SnapshotID,
				StorageGeneration:       normalized.StorageGeneration,
				SnapshotJournalSequence: source.SnapshotJournalSequence,
				TargetJournalSequence:   normalized.TargetJournalSequence,
				FencingToken:            normalized.FencingToken,
			},
		},
		EvictionEpoch: request.EvictionEpoch,
	}
	admission.rejoinPending[normalized.JoinerID] = replicaRejoinPending{
		request:  request,
		decision: decision,
	}
	return decision, nil
}

// CommitRejoin publishes an evicted member only after its exact bootstrap is
// active and caught up. The eviction epoch and topology generation are both
// checked while holding the admission lock.
func (admission *ReplicaJoinAdmission) CommitRejoin(decision ReplicaRejoinDecision, bootstrap SnapshotWALBootstrapState) (ReplicaJoinMember, error) {
	if admission == nil {
		return ReplicaJoinMember{}, ErrReplicaJoinAdmissionNil
	}
	if bootstrap.Phase != SnapshotWALBootstrapPhaseActive {
		return ReplicaJoinMember{}, ErrReplicaJoinAdmissionBootstrapNotActive
	}
	if bootstrap.Plan != decision.BootstrapPlan || bootstrap.AppliedJournalSequence < decision.BootstrapPlan.TargetJournalSequence {
		return ReplicaJoinMember{}, ErrReplicaJoinAdmissionBootstrapMismatch
	}

	admission.mu.Lock()
	defer admission.mu.Unlock()
	pending, exists := admission.rejoinPending[decision.JoinerID]
	if !exists || !reflect.DeepEqual(pending.decision, decision) {
		return ReplicaJoinMember{}, ErrReplicaJoinAdmissionConflict
	}
	eviction, exists := admission.evicted[decision.JoinerID]
	if !exists {
		return ReplicaJoinMember{}, ErrReplicaJoinAdmissionNotEvicted
	}
	if decision.EvictionEpoch != eviction.EvictionEpoch {
		return ReplicaJoinMember{}, ErrReplicaJoinAdmissionStaleEviction
	}
	if decision.TopologyGeneration != admission.generation+1 {
		return ReplicaJoinMember{}, ErrReplicaJoinAdmissionGeneration
	}
	for nodeID, member := range admission.members {
		if nodeID == decision.JoinerID {
			return ReplicaJoinMember{}, ErrReplicaJoinAdmissionAlreadyPresent
		}
		if member.Address == decision.Address {
			return ReplicaJoinMember{}, ErrReplicaJoinAdmissionAddressInUse
		}
	}
	delete(admission.rejoinPending, decision.JoinerID)
	delete(admission.evicted, decision.JoinerID)
	admission.generation = decision.TopologyGeneration
	member := ReplicaJoinMember{
		NodeID:            decision.JoinerID,
		Address:           decision.Address,
		SourceID:          decision.SourceID,
		SourceAddress:     decision.SourceAddress,
		StorageGeneration: decision.BootstrapPlan.StorageGeneration,
		Role:              decision.Role,
		Generation:        admission.generation,
	}
	admission.members[member.NodeID] = member
	return member, nil
}

// AbortRejoin removes a pending rejoin but deliberately retains the eviction
// tombstone, so the identity remains fenced until a fresh exact rejoin passes.
func (admission *ReplicaJoinAdmission) AbortRejoin(decision ReplicaRejoinDecision) error {
	if admission == nil {
		return ErrReplicaJoinAdmissionNil
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	pending, exists := admission.rejoinPending[decision.JoinerID]
	if !exists || !reflect.DeepEqual(pending.decision, decision) {
		return ErrReplicaJoinAdmissionConflict
	}
	delete(admission.rejoinPending, decision.JoinerID)
	return nil
}

// RecoverySnapshot returns a detached node-sorted view of retained eviction
// fences and pending rejoin count.
func (admission *ReplicaJoinAdmission) RecoverySnapshot() ReplicaRecoverySnapshot {
	if admission == nil {
		return ReplicaRecoverySnapshot{}
	}
	admission.mu.RLock()
	defer admission.mu.RUnlock()
	ids := make([]string, 0, len(admission.evicted))
	for nodeID := range admission.evicted {
		ids = append(ids, nodeID)
	}
	sort.Strings(ids)
	snapshot := ReplicaRecoverySnapshot{
		Generation:     admission.generation,
		Evictions:      make([]ReplicaEviction, 0, len(ids)),
		PendingRejoins: len(admission.rejoinPending),
	}
	for _, nodeID := range ids {
		snapshot.Evictions = append(snapshot.Evictions, admission.evicted[nodeID])
	}
	return snapshot
}
