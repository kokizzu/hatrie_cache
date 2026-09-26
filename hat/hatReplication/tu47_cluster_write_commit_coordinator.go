package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const (
	// MaxClusterWriteCommitCoordinatorSnapshotBytes bounds one durable
	// coordinator snapshot before its file envelope is added.
	MaxClusterWriteCommitCoordinatorSnapshotBytes = 64 << 20
	maxClusterWriteCommitCoordinatorStringBytes   = 1 << 20
)

var (
	ErrClusterWriteCommitCoordinatorInvalid              = errors.New("hatReplication: invalid coordinator state")
	ErrClusterWriteCommitCoordinatorStateStoreInvalid    = errors.New("hatReplication: coordinator state store is invalid")
	ErrClusterWriteCommitCoordinatorStateStoreSaveFailed = errors.New("hatReplication: coordinator state store save failed")
	ErrClusterWriteCommitCoordinatorSnapshotInvalid      = errors.New("hatReplication: coordinator snapshot is invalid")
)

// ClusterWriteCommitCoordinatorPhase is the durable coordinator boundary for
// one distributed write. CommitStarted and OutcomeUnknown require participant
// reconciliation rather than an unsafe blind retry.
type ClusterWriteCommitCoordinatorPhase uint8

const (
	ClusterWriteCommitCoordinatorProposed       ClusterWriteCommitCoordinatorPhase = 1
	ClusterWriteCommitCoordinatorPrepared       ClusterWriteCommitCoordinatorPhase = 2
	ClusterWriteCommitCoordinatorCommitStarted  ClusterWriteCommitCoordinatorPhase = 3
	ClusterWriteCommitCoordinatorCommitted      ClusterWriteCommitCoordinatorPhase = 4
	ClusterWriteCommitCoordinatorAbortStarted   ClusterWriteCommitCoordinatorPhase = 5
	ClusterWriteCommitCoordinatorAborted        ClusterWriteCommitCoordinatorPhase = 6
	ClusterWriteCommitCoordinatorOutcomeUnknown ClusterWriteCommitCoordinatorPhase = 7
)

// ClusterWriteCommitCoordinatorSnapshot is the durable coordinator intent and
// phase report. Nodes and attempts use the same input order as the two-phase
// operation, making recovery and reconciliation deterministic.
type ClusterWriteCommitCoordinatorSnapshot struct {
	Proposal ClusterWriteCommitProposal
	Nodes    []string
	Attempts []ClusterWriteCommitAttempt
	Phase    ClusterWriteCommitCoordinatorPhase
}

// ClusterWriteCommitCoordinatorStateStore persists coordinator boundaries.
// Save must durably replace the supplied snapshot before returning success.
type ClusterWriteCommitCoordinatorStateStore interface {
	Save(context.Context, ClusterWriteCommitCoordinatorSnapshot) error
}

// ExecuteClusterWriteCommitWithStateStore runs the existing two-phase write
// protocol while persisting every safety boundary. The original
// ExecuteClusterWriteCommit remains the zero-overhead opt-out path.
func ExecuteClusterWriteCommitWithStateStore(
	ctx context.Context,
	nodes []string,
	proposal ClusterWriteCommitProposal,
	prepare ClusterWriteCommitPrepareFunc,
	commit ClusterWriteCommitCommitFunc,
	abort ClusterWriteCommitAbortFunc,
	stateStore ClusterWriteCommitCoordinatorStateStore,
) (ClusterWriteCommitResult, error) {
	result := ClusterWriteCommitResult{Proposal: proposal}
	if stateStore == nil {
		return result, ErrClusterWriteCommitCoordinatorStateStoreInvalid
	}
	return executeClusterWriteCommit(ctx, nodes, proposal, prepare, commit, abort, stateStore)
}

func persistClusterWriteCommitCoordinatorSnapshot(
	ctx context.Context,
	stateStore ClusterWriteCommitCoordinatorStateStore,
	proposal ClusterWriteCommitProposal,
	nodes []string,
	attempts []ClusterWriteCommitAttempt,
	phase ClusterWriteCommitCoordinatorPhase,
) error {
	if stateStore == nil {
		return nil
	}
	snapshot := ClusterWriteCommitCoordinatorSnapshot{
		Proposal: proposal,
		Nodes:    append([]string(nil), nodes...),
		Attempts: append([]ClusterWriteCommitAttempt(nil), attempts...),
		Phase:    phase,
	}
	normalized, err := normalizeClusterWriteCommitCoordinatorSnapshot(snapshot)
	if err != nil {
		return err
	}
	if err := stateStore.Save(ctx, normalized); err != nil {
		return errors.Join(ErrClusterWriteCommitCoordinatorStateStoreSaveFailed, err)
	}
	return nil
}

func validateClusterWriteCommitCoordinatorSnapshot(snapshot ClusterWriteCommitCoordinatorSnapshot) error {
	_, err := normalizeClusterWriteCommitCoordinatorSnapshot(snapshot)
	return err
}

func normalizeClusterWriteCommitCoordinatorSnapshot(snapshot ClusterWriteCommitCoordinatorSnapshot) (ClusterWriteCommitCoordinatorSnapshot, error) {
	snapshot.Proposal.TransactionID = strings.TrimSpace(snapshot.Proposal.TransactionID)
	if snapshot.Proposal.TransactionID == "" || len(snapshot.Proposal.TransactionID) > maxClusterWriteCommitCoordinatorStringBytes {
		return ClusterWriteCommitCoordinatorSnapshot{}, ErrClusterWriteCommitCoordinatorSnapshotInvalid
	}
	nodes, err := normalizeClusterWriteCommitNodes(snapshot.Nodes)
	if err != nil {
		return ClusterWriteCommitCoordinatorSnapshot{}, errors.Join(ErrClusterWriteCommitCoordinatorSnapshotInvalid, err)
	}
	if len(snapshot.Attempts) != len(nodes) {
		return ClusterWriteCommitCoordinatorSnapshot{}, fmt.Errorf("%w: attempt count %d does not match node count %d", ErrClusterWriteCommitCoordinatorSnapshotInvalid, len(snapshot.Attempts), len(nodes))
	}
	if !validClusterWriteCommitCoordinatorPhase(snapshot.Phase) {
		return ClusterWriteCommitCoordinatorSnapshot{}, fmt.Errorf("%w: phase %d", ErrClusterWriteCommitCoordinatorSnapshotInvalid, snapshot.Phase)
	}
	snapshot.Nodes = append([]string(nil), nodes...)
	snapshot.Attempts = append([]ClusterWriteCommitAttempt(nil), snapshot.Attempts...)
	for index, attempt := range snapshot.Attempts {
		attempt.Node = strings.TrimSpace(attempt.Node)
		if attempt.Node != nodes[index] || len(attempt.Node) > maxClusterWriteCommitCoordinatorStringBytes ||
			len(attempt.PrepareError) > maxClusterWriteCommitCoordinatorStringBytes ||
			len(attempt.AbortError) > maxClusterWriteCommitCoordinatorStringBytes ||
			len(attempt.CommitError) > maxClusterWriteCommitCoordinatorStringBytes {
			return ClusterWriteCommitCoordinatorSnapshot{}, fmt.Errorf("%w: invalid attempt %d", ErrClusterWriteCommitCoordinatorSnapshotInvalid, index)
		}
		snapshot.Attempts[index] = attempt
	}
	return snapshot, nil
}

func validClusterWriteCommitCoordinatorPhase(phase ClusterWriteCommitCoordinatorPhase) bool {
	return phase >= ClusterWriteCommitCoordinatorProposed && phase <= ClusterWriteCommitCoordinatorOutcomeUnknown
}
