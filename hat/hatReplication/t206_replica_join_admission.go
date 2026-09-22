package hatReplication

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	// DefaultReplicaJoinAdmissionMaxMembers bounds active and pending joins.
	DefaultReplicaJoinAdmissionMaxMembers = 256
	// DefaultReplicaJoinAdmissionMaxCandidates bounds source candidates in one join request.
	DefaultReplicaJoinAdmissionMaxCandidates = 64
	// MaxReplicaJoinAdmissionMaxMembers prevents an accidental unbounded membership map.
	MaxReplicaJoinAdmissionMaxMembers = 1 << 16
	// MaxReplicaJoinAdmissionMaxCandidates prevents an accidental unbounded candidate list.
	MaxReplicaJoinAdmissionMaxCandidates = 1 << 12
	// MaxReplicaJoinNodeIDBytes bounds stable node identities.
	MaxReplicaJoinNodeIDBytes = 256
	// MaxReplicaJoinAddressBytes bounds transport endpoint addresses.
	MaxReplicaJoinAddressBytes = 1024
	// MaxReplicaJoinSnapshotIDBytes bounds immutable snapshot identities.
	MaxReplicaJoinSnapshotIDBytes = 256
)

var (
	// ErrReplicaJoinAdmissionNil indicates a method call on a nil admission registry.
	ErrReplicaJoinAdmissionNil = errors.New("hatReplication: replica join admission is nil")
	// ErrReplicaJoinAdmissionInvalid identifies malformed join or candidate data.
	ErrReplicaJoinAdmissionInvalid = errors.New("hatReplication: replica join admission request is invalid")
	// ErrReplicaJoinAdmissionAlreadyPresent identifies a node ID already in the topology.
	ErrReplicaJoinAdmissionAlreadyPresent = errors.New("hatReplication: replica join node is already present")
	// ErrReplicaJoinAdmissionAddressInUse identifies an endpoint already claimed by another node.
	ErrReplicaJoinAdmissionAddressInUse = errors.New("hatReplication: replica join address is already in use")
	// ErrReplicaJoinAdmissionConflict identifies a retry with different join data.
	ErrReplicaJoinAdmissionConflict = errors.New("hatReplication: replica join admission conflicts with a pending decision")
	// ErrReplicaJoinAdmissionNoSource indicates that no eligible deterministic source exists.
	ErrReplicaJoinAdmissionNoSource = errors.New("hatReplication: replica join has no eligible source")
	// ErrReplicaJoinAdmissionLimit indicates that the bounded membership capacity is full.
	ErrReplicaJoinAdmissionLimit = errors.New("hatReplication: replica join admission member limit reached")
	// ErrReplicaJoinAdmissionGeneration identifies a stale topology generation.
	ErrReplicaJoinAdmissionGeneration = errors.New("hatReplication: replica join admission generation mismatch")
	// ErrReplicaJoinAdmissionBootstrapNotActive indicates that membership cannot be published yet.
	ErrReplicaJoinAdmissionBootstrapNotActive = errors.New("hatReplication: replica join bootstrap is not active")
	// ErrReplicaJoinAdmissionBootstrapMismatch identifies a bootstrap state for another decision.
	ErrReplicaJoinAdmissionBootstrapMismatch = errors.New("hatReplication: replica join bootstrap plan mismatch")
)

// ReplicaJoinAdmissionOptions bounds one admission registry. Zero values select
// the documented defaults. The registry is transport-neutral and has no cost
// until a caller constructs it and feeds a join request.
type ReplicaJoinAdmissionOptions struct {
	MaxMembers    int
	MaxCandidates int
}

// ReplicaJoinCandidate describes one possible snapshot/WAL source.
type ReplicaJoinCandidate struct {
	NodeID                  string
	Address                 string
	Healthy                 bool
	StorageGeneration       uint64
	SnapshotJournalSequence uint64
	AppliedJournalSequence  uint64
	AvailableThrough        uint64
}

// ReplicaJoinRequest describes a new node's identity and bootstrap target.
// Candidates are normalized and ordered before source selection.
type ReplicaJoinRequest struct {
	JoinerID              string
	Address               string
	SnapshotID            string
	StorageGeneration     uint64
	TargetJournalSequence uint64
	FencingToken          uint64
	Candidates            []ReplicaJoinCandidate
}

// ReplicaJoinDecision is the immutable result of a deterministic prepare.
// TopologyGeneration is the generation at which this decision may commit.
type ReplicaJoinDecision struct {
	JoinerID           string
	Address            string
	SourceID           string
	SourceAddress      string
	BootstrapPlan      SnapshotWALBootstrapPlan
	TopologyGeneration uint64
}

// ReplicaJoinMember is a detached active topology member.
type ReplicaJoinMember struct {
	NodeID            string
	Address           string
	SourceID          string
	SourceAddress     string
	StorageGeneration uint64
	Generation        uint64
}

// ReplicaJoinAdmissionSnapshot is a detached, node-sorted registry view.
type ReplicaJoinAdmissionSnapshot struct {
	Generation uint64
	Members    []ReplicaJoinMember
	Pending    int
}

type replicaJoinPending struct {
	request  ReplicaJoinRequest
	decision ReplicaJoinDecision
}

// ReplicaJoinAdmission coordinates deterministic join admission. It does not
// copy snapshots, replay WAL records, perform network I/O, or publish routes;
// callers must activate the existing SnapshotWALBootstrapCoordinator first and
// pass its active state to Commit.
type ReplicaJoinAdmission struct {
	mu            sync.RWMutex
	maxMembers    int
	maxCandidates int
	generation    uint64
	members       map[string]ReplicaJoinMember
	pending       map[string]replicaJoinPending
}

// NewReplicaJoinAdmission creates a bounded join admission registry.
func NewReplicaJoinAdmission(options ReplicaJoinAdmissionOptions) (*ReplicaJoinAdmission, error) {
	maxMembers := options.MaxMembers
	if maxMembers == 0 {
		maxMembers = DefaultReplicaJoinAdmissionMaxMembers
	}
	maxCandidates := options.MaxCandidates
	if maxCandidates == 0 {
		maxCandidates = DefaultReplicaJoinAdmissionMaxCandidates
	}
	if maxMembers < 1 || maxMembers > MaxReplicaJoinAdmissionMaxMembers || maxCandidates < 1 || maxCandidates > MaxReplicaJoinAdmissionMaxCandidates {
		return nil, ErrReplicaJoinAdmissionInvalid
	}
	return &ReplicaJoinAdmission{
		maxMembers:    maxMembers,
		maxCandidates: maxCandidates,
		members:       make(map[string]ReplicaJoinMember, maxMembers),
		pending:       make(map[string]replicaJoinPending),
	}, nil
}

// Prepare validates a request and selects the healthy candidate with the
// highest applied journal sequence. Equal progress is resolved by NodeID, so
// retries and different candidate input orders produce the same decision.
func (admission *ReplicaJoinAdmission) Prepare(request ReplicaJoinRequest) (ReplicaJoinDecision, error) {
	if admission == nil {
		return ReplicaJoinDecision{}, ErrReplicaJoinAdmissionNil
	}
	normalized, err := normalizeReplicaJoinRequest(request, admission.maxCandidates)
	if err != nil {
		return ReplicaJoinDecision{}, err
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()

	if _, exists := admission.members[normalized.JoinerID]; exists {
		return ReplicaJoinDecision{}, ErrReplicaJoinAdmissionAlreadyPresent
	}
	if pending, exists := admission.pending[normalized.JoinerID]; exists {
		if reflect.DeepEqual(pending.request, normalized) {
			return pending.decision, nil
		}
		return ReplicaJoinDecision{}, ErrReplicaJoinAdmissionConflict
	}
	for nodeID, member := range admission.members {
		if member.Address == normalized.Address {
			return ReplicaJoinDecision{}, fmt.Errorf("%w: active node %q", ErrReplicaJoinAdmissionAddressInUse, nodeID)
		}
	}
	for nodeID, pending := range admission.pending {
		if pending.request.Address == normalized.Address {
			return ReplicaJoinDecision{}, fmt.Errorf("%w: pending node %q", ErrReplicaJoinAdmissionAddressInUse, nodeID)
		}
	}
	if len(admission.members)+len(admission.pending) >= admission.maxMembers {
		return ReplicaJoinDecision{}, ErrReplicaJoinAdmissionLimit
	}
	source, err := selectReplicaJoinSource(normalized)
	if err != nil {
		return ReplicaJoinDecision{}, err
	}
	decision := ReplicaJoinDecision{
		JoinerID:           normalized.JoinerID,
		Address:            normalized.Address,
		SourceID:           source.NodeID,
		SourceAddress:      source.Address,
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
	}
	admission.pending[normalized.JoinerID] = replicaJoinPending{
		request:  normalized,
		decision: decision,
	}
	return decision, nil
}

// Commit publishes an admitted member only after its exact bootstrap plan is
// active and caught up. A stale decision cannot publish after another member
// advances the topology generation.
func (admission *ReplicaJoinAdmission) Commit(decision ReplicaJoinDecision, bootstrap SnapshotWALBootstrapState) (ReplicaJoinMember, error) {
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
	pending, exists := admission.pending[decision.JoinerID]
	if !exists || !reflect.DeepEqual(pending.decision, decision) {
		return ReplicaJoinMember{}, ErrReplicaJoinAdmissionConflict
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
	delete(admission.pending, decision.JoinerID)
	admission.generation = decision.TopologyGeneration
	member := ReplicaJoinMember{
		NodeID:            decision.JoinerID,
		Address:           decision.Address,
		SourceID:          decision.SourceID,
		SourceAddress:     decision.SourceAddress,
		StorageGeneration: decision.BootstrapPlan.StorageGeneration,
		Generation:        admission.generation,
	}
	admission.members[member.NodeID] = member
	return member, nil
}

// Abort removes an exact pending decision without changing the active
// topology generation. The decision cannot be committed after abort.
func (admission *ReplicaJoinAdmission) Abort(decision ReplicaJoinDecision) error {
	if admission == nil {
		return ErrReplicaJoinAdmissionNil
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	pending, exists := admission.pending[decision.JoinerID]
	if !exists || !reflect.DeepEqual(pending.decision, decision) {
		return ErrReplicaJoinAdmissionConflict
	}
	delete(admission.pending, decision.JoinerID)
	return nil
}

// Snapshot returns a detached node-sorted view of active members and pending
// count. Mutating the returned slice does not change admission state.
func (admission *ReplicaJoinAdmission) Snapshot() ReplicaJoinAdmissionSnapshot {
	if admission == nil {
		return ReplicaJoinAdmissionSnapshot{}
	}
	admission.mu.RLock()
	defer admission.mu.RUnlock()
	ids := make([]string, 0, len(admission.members))
	for nodeID := range admission.members {
		ids = append(ids, nodeID)
	}
	sort.Strings(ids)
	snapshot := ReplicaJoinAdmissionSnapshot{
		Generation: admission.generation,
		Members:    make([]ReplicaJoinMember, 0, len(ids)),
		Pending:    len(admission.pending),
	}
	for _, nodeID := range ids {
		snapshot.Members = append(snapshot.Members, admission.members[nodeID])
	}
	return snapshot
}

func normalizeReplicaJoinRequest(request ReplicaJoinRequest, maxCandidates int) (ReplicaJoinRequest, error) {
	var err error
	if request.JoinerID, err = normalizeReplicaJoinValue(request.JoinerID, MaxReplicaJoinNodeIDBytes, "joiner ID"); err != nil {
		return ReplicaJoinRequest{}, err
	}
	if request.Address, err = normalizeReplicaJoinValue(request.Address, MaxReplicaJoinAddressBytes, "address"); err != nil {
		return ReplicaJoinRequest{}, err
	}
	if request.SnapshotID, err = normalizeReplicaJoinValue(request.SnapshotID, MaxReplicaJoinSnapshotIDBytes, "snapshot ID"); err != nil {
		return ReplicaJoinRequest{}, err
	}
	if request.StorageGeneration == 0 || request.FencingToken == 0 {
		return ReplicaJoinRequest{}, ErrReplicaJoinAdmissionInvalid
	}
	if len(request.Candidates) == 0 || len(request.Candidates) > maxCandidates {
		return ReplicaJoinRequest{}, ErrReplicaJoinAdmissionInvalid
	}
	request.Candidates = append([]ReplicaJoinCandidate(nil), request.Candidates...)
	seenIDs := make(map[string]struct{}, len(request.Candidates))
	seenAddresses := make(map[string]struct{}, len(request.Candidates))
	for index := range request.Candidates {
		candidate := &request.Candidates[index]
		if candidate.NodeID, err = normalizeReplicaJoinValue(candidate.NodeID, MaxReplicaJoinNodeIDBytes, "candidate node ID"); err != nil {
			return ReplicaJoinRequest{}, err
		}
		if candidate.Address, err = normalizeReplicaJoinValue(candidate.Address, MaxReplicaJoinAddressBytes, "candidate address"); err != nil {
			return ReplicaJoinRequest{}, err
		}
		if candidate.NodeID == request.JoinerID || candidate.Address == request.Address || candidate.StorageGeneration == 0 || candidate.SnapshotJournalSequence > candidate.AppliedJournalSequence || candidate.AppliedJournalSequence > candidate.AvailableThrough {
			return ReplicaJoinRequest{}, ErrReplicaJoinAdmissionInvalid
		}
		if _, exists := seenIDs[candidate.NodeID]; exists {
			return ReplicaJoinRequest{}, ErrReplicaJoinAdmissionInvalid
		}
		if _, exists := seenAddresses[candidate.Address]; exists {
			return ReplicaJoinRequest{}, ErrReplicaJoinAdmissionInvalid
		}
		seenIDs[candidate.NodeID] = struct{}{}
		seenAddresses[candidate.Address] = struct{}{}
	}
	sort.Slice(request.Candidates, func(left, right int) bool {
		if request.Candidates[left].NodeID != request.Candidates[right].NodeID {
			return request.Candidates[left].NodeID < request.Candidates[right].NodeID
		}
		return request.Candidates[left].Address < request.Candidates[right].Address
	})
	return request, nil
}

func selectReplicaJoinSource(request ReplicaJoinRequest) (ReplicaJoinCandidate, error) {
	var selected ReplicaJoinCandidate
	found := false
	for _, candidate := range request.Candidates {
		if !candidate.Healthy || candidate.StorageGeneration != request.StorageGeneration || candidate.SnapshotJournalSequence > request.TargetJournalSequence || candidate.AvailableThrough < request.TargetJournalSequence {
			continue
		}
		if !found || candidate.AppliedJournalSequence > selected.AppliedJournalSequence || (candidate.AppliedJournalSequence == selected.AppliedJournalSequence && candidate.NodeID < selected.NodeID) {
			selected = candidate
			found = true
		}
	}
	if !found {
		return ReplicaJoinCandidate{}, ErrReplicaJoinAdmissionNoSource
	}
	return selected, nil
}

func normalizeReplicaJoinValue(value string, maxBytes int, label string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || len(trimmed) > maxBytes || trimmed != value || !utf8.ValidString(trimmed) || strings.IndexByte(trimmed, 0) >= 0 {
		return "", fmt.Errorf("%w: %s", ErrReplicaJoinAdmissionInvalid, label)
	}
	return trimmed, nil
}
