package hatReplication

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrReplicaBootstrapInvalid indicates malformed join or source metadata.
	ErrReplicaBootstrapInvalid = errors.New("hatReplication: replica bootstrap data is invalid")
	// ErrReplicaBootstrapNoSource indicates that no source satisfies the join
	// request and bounded replay policy.
	ErrReplicaBootstrapNoSource = errors.New("hatReplication: no eligible replica bootstrap source")
	// ErrReplicaBootstrapDuplicateSource indicates ambiguous source identity.
	ErrReplicaBootstrapDuplicateSource = errors.New("hatReplication: duplicate replica bootstrap source")
	// ErrReplicaBootstrapWorkflowNil indicates a nil workflow receiver.
	ErrReplicaBootstrapWorkflowNil = errors.New("hatReplication: replica bootstrap workflow is nil")
)

// ReplicaBootstrapSource is the immutable source advertisement used to plan a
// replica join. A source is eligible only when Ready is true and its snapshot
// boundary can be replayed within the request's WAL gap bound.
type ReplicaBootstrapSource struct {
	NodeID                  string
	Address                 string
	Region                  string
	Ready                   bool
	SnapshotID              string
	StorageGeneration       uint64
	SnapshotJournalSequence uint64
	CurrentJournalSequence  uint64
	FencingToken            uint64
}

// ReplicaBootstrapRequest defines the deterministic admission constraints for
// a joining node. RequiredStorageGeneration may be zero when the caller has no
// generation preference; non-zero values require an exact match.
type ReplicaBootstrapRequest struct {
	JoinerID                  string
	RequiredStorageGeneration uint64
	MinimumJournalSequence    uint64
	PreferredRegions          []string
	MaxWALGap                 uint64
}

// ReplicaBootstrapPlan combines the selected source advertisement with the
// transport-neutral snapshot-plus-WAL plan.
type ReplicaBootstrapPlan struct {
	Source    ReplicaBootstrapSource
	Bootstrap SnapshotWALBootstrapPlan
	WALGap    uint64
}

const (
	replicaBootstrapInlineSourceLimit = 64
	replicaBootstrapInlineTableSize   = replicaBootstrapInlineSourceLimit * 2
)

// ReplicaBootstrapWorkflow binds a deterministic plan to the existing,
// fenced snapshot-plus-WAL lifecycle coordinator. File transfer, journal
// replay, health checks, and topology publication remain caller-owned side
// effects around the coordinator transitions.
type ReplicaBootstrapWorkflow struct {
	plan        ReplicaBootstrapPlan
	coordinator *SnapshotWALBootstrapCoordinator
}

// PlanReplicaBootstrap selects one source without depending on input order.
// Locality is ranked first, then the freshest source, the newest snapshot
// boundary, the highest storage generation, and finally lexical node and
// snapshot identifiers. This makes retries and independently ordered source
// advertisements produce the same join plan.
func PlanReplicaBootstrap(request ReplicaBootstrapRequest, sources []ReplicaBootstrapSource) (ReplicaBootstrapPlan, error) {
	joinerID, err := normalizeSnapshotWALBootstrapIdentifier(request.JoinerID, "joiner ID")
	if err != nil {
		return ReplicaBootstrapPlan{}, fmt.Errorf("%w: %v", ErrReplicaBootstrapInvalid, err)
	}
	maxWALGap := request.MaxWALGap
	if maxWALGap == 0 {
		maxWALGap = DefaultSnapshotWALBootstrapMaxGap
	}
	if maxWALGap > MaxSnapshotWALBootstrapMaxGap {
		return ReplicaBootstrapPlan{}, fmt.Errorf("%w: max WAL gap %d exceeds %d", ErrReplicaBootstrapInvalid, maxWALGap, MaxSnapshotWALBootstrapMaxGap)
	}

	// Most replica sets are small. Keep duplicate detection in a bounded
	// open-addressed table on the stack; only unusually large advertisements
	// need a heap-backed map.
	var inlineSeen [replicaBootstrapInlineTableSize]string
	var inlineHashes [replicaBootstrapInlineTableSize]uint64
	inlineSeenCount := 0
	var overflowSeen map[string]struct{}
	var selected ReplicaBootstrapSource
	selectedRegionRank := len(request.PreferredRegions)
	selectedSet := false
	for _, advertised := range sources {
		nodeID, err := normalizeSnapshotWALBootstrapIdentifier(advertised.NodeID, "source node ID")
		if err != nil {
			return ReplicaBootstrapPlan{}, fmt.Errorf("%w: %v", ErrReplicaBootstrapInvalid, err)
		}
		hash := uint64(0)
		duplicate := false
		if overflowSeen != nil {
			_, duplicate = overflowSeen[nodeID]
		} else {
			hash = replicaBootstrapStringHash(nodeID)
			slot := int(hash & (replicaBootstrapInlineTableSize - 1))
			for probe := 0; probe < replicaBootstrapInlineTableSize; probe++ {
				if inlineSeen[slot] == "" {
					break
				}
				if inlineHashes[slot] == hash && inlineSeen[slot] == nodeID {
					duplicate = true
					break
				}
				slot = (slot + 1) & (replicaBootstrapInlineTableSize - 1)
			}
		}
		if duplicate {
			return ReplicaBootstrapPlan{}, fmt.Errorf("%w: %s", ErrReplicaBootstrapDuplicateSource, nodeID)
		}
		if overflowSeen != nil {
			overflowSeen[nodeID] = struct{}{}
		} else if inlineSeenCount < replicaBootstrapInlineSourceLimit {
			slot := int(hash & (replicaBootstrapInlineTableSize - 1))
			for inlineSeen[slot] != "" {
				slot = (slot + 1) & (replicaBootstrapInlineTableSize - 1)
			}
			inlineSeen[slot] = nodeID
			inlineHashes[slot] = hash
			inlineSeenCount++
		} else {
			overflowSeen = make(map[string]struct{}, len(sources)-replicaBootstrapInlineSourceLimit+1)
			for index := range inlineSeen {
				if inlineSeen[index] != "" {
					overflowSeen[inlineSeen[index]] = struct{}{}
				}
			}
			overflowSeen[nodeID] = struct{}{}
		}
		if nodeID == joinerID || !advertised.Ready {
			continue
		}
		if request.RequiredStorageGeneration != 0 && advertised.StorageGeneration != request.RequiredStorageGeneration {
			continue
		}
		if advertised.CurrentJournalSequence < request.MinimumJournalSequence || advertised.CurrentJournalSequence < advertised.SnapshotJournalSequence {
			continue
		}
		if advertised.CurrentJournalSequence-advertised.SnapshotJournalSequence > maxWALGap {
			continue
		}
		if advertised.StorageGeneration == 0 || advertised.FencingToken == 0 {
			continue
		}
		snapshotID, err := normalizeSnapshotWALBootstrapIdentifier(advertised.SnapshotID, "snapshot ID")
		if err != nil {
			continue
		}
		advertised.NodeID = nodeID
		advertised.SnapshotID = snapshotID
		advertised.Address = strings.TrimSpace(advertised.Address)
		advertised.Region = strings.TrimSpace(advertised.Region)
		regionRank := readReplicaRegionRank(advertised.Region, request.PreferredRegions)
		if !selectedSet || betterReplicaBootstrapSource(advertised, regionRank, selected, selectedRegionRank) {
			selected = advertised
			selectedRegionRank = regionRank
			selectedSet = true
		}
	}
	if !selectedSet {
		return ReplicaBootstrapPlan{}, ErrReplicaBootstrapNoSource
	}

	bootstrap := SnapshotWALBootstrapPlan{
		JoinerID:                joinerID,
		SourceID:                selected.NodeID,
		SnapshotID:              selected.SnapshotID,
		StorageGeneration:       selected.StorageGeneration,
		SnapshotJournalSequence: selected.SnapshotJournalSequence,
		TargetJournalSequence:   selected.CurrentJournalSequence,
		FencingToken:            selected.FencingToken,
	}
	return ReplicaBootstrapPlan{
		Source:    selected,
		Bootstrap: bootstrap,
		WALGap:    selected.CurrentJournalSequence - selected.SnapshotJournalSequence,
	}, nil
}

func betterReplicaBootstrapSource(candidate ReplicaBootstrapSource, candidateRegionRank int, selected ReplicaBootstrapSource, selectedRegionRank int) bool {
	if candidateRegionRank != selectedRegionRank {
		return candidateRegionRank < selectedRegionRank
	}
	if candidate.CurrentJournalSequence != selected.CurrentJournalSequence {
		return candidate.CurrentJournalSequence > selected.CurrentJournalSequence
	}
	if candidate.SnapshotJournalSequence != selected.SnapshotJournalSequence {
		return candidate.SnapshotJournalSequence > selected.SnapshotJournalSequence
	}
	if candidate.StorageGeneration != selected.StorageGeneration {
		return candidate.StorageGeneration > selected.StorageGeneration
	}
	if candidate.NodeID != selected.NodeID {
		return candidate.NodeID < selected.NodeID
	}
	return candidate.SnapshotID < selected.SnapshotID
}

func replicaBootstrapStringHash(value string) uint64 {
	const (
		basis = uint64(1469598103934665603)
		prime = uint64(1099511628211)
	)
	hash := basis
	for index := 0; index < len(value); index++ {
		hash ^= uint64(value[index])
		hash *= prime
	}
	return hash
}

// NewReplicaBootstrapWorkflow validates a plan and creates its fenced
// lifecycle coordinator.
func NewReplicaBootstrapWorkflow(plan ReplicaBootstrapPlan) (*ReplicaBootstrapWorkflow, error) {
	if plan.Bootstrap.TargetJournalSequence < plan.Bootstrap.SnapshotJournalSequence {
		return nil, fmt.Errorf("%w: target journal sequence precedes snapshot", ErrReplicaBootstrapInvalid)
	}
	if plan.WALGap != plan.Bootstrap.TargetJournalSequence-plan.Bootstrap.SnapshotJournalSequence {
		return nil, fmt.Errorf("%w: WAL gap does not match bootstrap boundaries", ErrReplicaBootstrapInvalid)
	}
	if plan.Source.NodeID != plan.Bootstrap.SourceID || plan.Source.SnapshotID != plan.Bootstrap.SnapshotID || !plan.Source.Ready {
		return nil, fmt.Errorf("%w: source does not match bootstrap plan", ErrReplicaBootstrapInvalid)
	}
	if plan.Source.StorageGeneration != plan.Bootstrap.StorageGeneration || plan.Source.FencingToken != plan.Bootstrap.FencingToken {
		return nil, fmt.Errorf("%w: source metadata does not match bootstrap plan", ErrReplicaBootstrapInvalid)
	}
	if _, err := normalizeSnapshotWALBootstrapPlan(plan.Bootstrap, DefaultSnapshotWALBootstrapMaxGap); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrReplicaBootstrapInvalid, err)
	}
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{})
	if err != nil {
		return nil, err
	}
	return &ReplicaBootstrapWorkflow{plan: plan, coordinator: coordinator}, nil
}

// NewReplicaBootstrapWorkflowFromSources selects a deterministic source and
// binds the resulting plan to its lifecycle coordinator in one operation.
func NewReplicaBootstrapWorkflowFromSources(request ReplicaBootstrapRequest, sources []ReplicaBootstrapSource) (*ReplicaBootstrapWorkflow, error) {
	plan, err := PlanReplicaBootstrap(request, sources)
	if err != nil {
		return nil, err
	}
	return NewReplicaBootstrapWorkflow(plan)
}

// Plan returns the independent plan bound to the workflow.
func (workflow *ReplicaBootstrapWorkflow) Plan() ReplicaBootstrapPlan {
	if workflow == nil {
		return ReplicaBootstrapPlan{}
	}
	return workflow.plan
}

// Coordinator returns the lifecycle coordinator used by the workflow.
func (workflow *ReplicaBootstrapWorkflow) Coordinator() *SnapshotWALBootstrapCoordinator {
	if workflow == nil {
		return nil
	}
	return workflow.coordinator
}

// Begin starts the planned snapshot-plus-WAL lifecycle.
func (workflow *ReplicaBootstrapWorkflow) Begin() (SnapshotWALBootstrapState, error) {
	if workflow == nil {
		return SnapshotWALBootstrapState{}, ErrReplicaBootstrapWorkflowNil
	}
	return workflow.coordinator.Begin(workflow.plan.Bootstrap)
}
