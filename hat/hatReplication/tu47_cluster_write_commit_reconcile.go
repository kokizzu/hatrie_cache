package hatReplication

import (
	"errors"
	"fmt"
)

var (
	// ErrClusterWriteCommitParticipantReconcileOrder indicates that decisions
	// are not strictly ordered by normalized transaction ID.
	ErrClusterWriteCommitParticipantReconcileOrder = errors.New("hatReplication: participant reconciliation decisions are not ordered")
	// ErrClusterWriteCommitParticipantUnknownTransaction indicates that a
	// coordinator tried to reconcile a transaction absent from local state.
	ErrClusterWriteCommitParticipantUnknownTransaction = errors.New("hatReplication: participant reconciliation transaction is unknown")
)

// ClusterWriteCommitParticipantReconcileDecision is one coordinator decision
// for a prepared transaction. Decisions must be strictly ordered by
// Proposal.TransactionID; PreparedRecords returns that canonical order.
type ClusterWriteCommitParticipantReconcileDecision struct {
	Proposal ClusterWriteCommitProposal
	Phase    ClusterWriteCommitParticipantPhase
}

// ClusterWriteCommitParticipantReconcileResult reports one atomic batch.
type ClusterWriteCommitParticipantReconcileResult struct {
	Requested       int
	Committed       int
	Aborted         int
	AlreadyTerminal int
}

// PreparedRecords returns an independently owned, transaction-ID-ordered
// snapshot of all locally prepared transactions. The returned order is the
// zero-allocation input order expected by Reconcile.
func (participant *ClusterWriteCommitParticipant) PreparedRecords() []ClusterWriteCommitParticipantRecord {
	if participant == nil {
		return nil
	}
	participant.mu.RLock()
	records := make([]ClusterWriteCommitParticipantRecord, 0, len(participant.records))
	for _, record := range participant.records {
		if record.Phase == ClusterWriteCommitParticipantPrepared {
			records = append(records, record)
		}
	}
	participant.mu.RUnlock()
	sortClusterWriteCommitParticipantRecords(records)
	return records
}

// Reconcile applies an authoritative commit or abort decision batch. It first
// validates every decision and mutates no state on failure. Repeating a
// decision for the same terminal phase is idempotent; a conflicting terminal
// phase remains an error.
func (participant *ClusterWriteCommitParticipant) Reconcile(decisions []ClusterWriteCommitParticipantReconcileDecision) (ClusterWriteCommitParticipantReconcileResult, error) {
	result := ClusterWriteCommitParticipantReconcileResult{Requested: len(decisions)}
	if participant == nil {
		return result, ErrClusterWriteCommitParticipantInvalid
	}
	participant.mu.Lock()
	defer participant.mu.Unlock()
	if len(decisions) > participant.maxRecords {
		return result, ErrClusterWriteCommitParticipantCapacity
	}

	previousID := ""
	for index, decision := range decisions {
		proposal, err := normalizeClusterWriteCommitParticipantProposal(decision.Proposal)
		if err != nil {
			return result, err
		}
		if decision.Phase != ClusterWriteCommitParticipantCommitted && decision.Phase != ClusterWriteCommitParticipantAborted {
			return result, ErrClusterWriteCommitParticipantInvalid
		}
		if index > 0 && proposal.TransactionID <= previousID {
			return result, fmt.Errorf("%w: %q after %q", ErrClusterWriteCommitParticipantReconcileOrder, proposal.TransactionID, previousID)
		}
		previousID = proposal.TransactionID
		existing, ok := participant.records[proposal.TransactionID]
		if !ok {
			return result, fmt.Errorf("%w: %s", ErrClusterWriteCommitParticipantUnknownTransaction, proposal.TransactionID)
		}
		if existing.Proposal != proposal {
			return result, ErrClusterWriteCommitParticipantConflict
		}
		switch existing.Phase {
		case ClusterWriteCommitParticipantPrepared:
		case decision.Phase:
			result.AlreadyTerminal++
		case ClusterWriteCommitParticipantCommitted:
			return result, ErrClusterWriteCommitParticipantCommitted
		case ClusterWriteCommitParticipantAborted:
			return result, ErrClusterWriteCommitParticipantAborted
		default:
			return result, ErrClusterWriteCommitParticipantInvalid
		}
	}

	for _, decision := range decisions {
		proposal, _ := normalizeClusterWriteCommitParticipantProposal(decision.Proposal)
		existing := participant.records[proposal.TransactionID]
		if existing.Phase != ClusterWriteCommitParticipantPrepared {
			continue
		}
		existing.Phase = decision.Phase
		participant.records[proposal.TransactionID] = existing
		if decision.Phase == ClusterWriteCommitParticipantCommitted {
			result.Committed++
		} else {
			result.Aborted++
		}
	}
	return result, nil
}
