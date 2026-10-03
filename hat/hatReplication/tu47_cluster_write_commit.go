package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// MaxClusterWriteCommitNodes bounds one two-phase write operation.
	MaxClusterWriteCommitNodes = 1 << 10
)

var (
	// ErrClusterWriteCommitInvalid indicates invalid participants, proposal, or
	// phase callbacks.
	ErrClusterWriteCommitInvalid = errors.New("hatReplication: cluster write commit input is invalid")
	// ErrClusterWriteCommitPrepareFailed indicates that at least one participant
	// did not prepare and no commit callback was started.
	ErrClusterWriteCommitPrepareFailed = errors.New("hatReplication: cluster write commit prepare failed")
	// ErrClusterWriteCommitOutcomeUnknown indicates that commit callbacks were
	// started and at least one participant did not confirm commit. The caller
	// must reconcile by transaction ID; it must not retry blindly.
	ErrClusterWriteCommitOutcomeUnknown = errors.New("hatReplication: cluster write commit outcome is unknown")
)

// ClusterWriteCommitProposal identifies one idempotent distributed write.
// TransactionID must be stable across retries and is the caller's deduplication
// key. Sequence, FenceToken, and PayloadDigest let a caller bind the operation
// to its own journal or fencing protocol without this package interpreting the
// payload.
type ClusterWriteCommitProposal struct {
	TransactionID string
	Sequence      uint64
	FenceToken    uint64
	PayloadDigest [32]byte
}

// ClusterWriteCommitPrepareFunc reserves the write on one participant without
// making it visible. It must be idempotent for the proposal transaction ID.
type ClusterWriteCommitPrepareFunc func(context.Context, string, ClusterWriteCommitProposal) error

// ClusterWriteCommitCommitFunc makes a prepared write visible. Once any commit
// callback starts, the coordinator never invokes an abort callback.
type ClusterWriteCommitCommitFunc func(context.Context, string, ClusterWriteCommitProposal) error

// ClusterWriteCommitAbortFunc releases a participant's prepared write after a
// prepare-phase failure. It must be idempotent because the caller may retry
// reconciliation after a process or network failure.
type ClusterWriteCommitAbortFunc func(context.Context, string, ClusterWriteCommitProposal) error

// ClusterWriteCommitAttempt reports one participant in input order. Error text
// is copied into the result so the result is safe to retain after callbacks
// return.
type ClusterWriteCommitAttempt struct {
	Node         string
	Prepared     bool
	PrepareError string
	Aborted      bool
	AbortError   string
	Committed    bool
	CommitError  string
}

// ClusterWriteCommitResult reports the phase boundary and each participant's
// outcome. Prepared is true only when every participant prepared. Committed is
// true only when every participant committed. OutcomeUnknown is true when the
// commit phase started but did not complete for every participant.
type ClusterWriteCommitResult struct {
	Proposal       ClusterWriteCommitProposal
	Attempts       []ClusterWriteCommitAttempt
	Prepared       bool
	PreparedCount  int
	AbortedCount   int
	Committed      bool
	CommittedCount int
	OutcomeUnknown bool
}

// ExecuteClusterWriteCommit runs an opt-in two-phase commit across every named
// participant. All prepare callbacks finish before any commit callback starts.
// A prepare failure invokes abort for only successful prepares. Once commit
// begins, a partial failure returns ErrClusterWriteCommitOutcomeUnknown and
// leaves reconciliation to the caller; no unsafe rollback is attempted.
func ExecuteClusterWriteCommit(
	ctx context.Context,
	nodes []string,
	proposal ClusterWriteCommitProposal,
	prepare ClusterWriteCommitPrepareFunc,
	commit ClusterWriteCommitCommitFunc,
	abort ClusterWriteCommitAbortFunc,
) (ClusterWriteCommitResult, error) {
	return executeClusterWriteCommit(ctx, nodes, proposal, prepare, commit, abort, nil)
}

// ExecuteClusterWriteCommitDurable runs the same two-phase protocol while
// persisting coordinator phase transitions through recorder. The recorder is
// written before each phase starts, so a restarted coordinator can list the
// last safe phase or reconcile an indeterminate commit by transaction ID.
// Existing ExecuteClusterWriteCommit callers remain entirely in-memory.
func ExecuteClusterWriteCommitDurable(
	ctx context.Context,
	nodes []string,
	proposal ClusterWriteCommitProposal,
	prepare ClusterWriteCommitPrepareFunc,
	commit ClusterWriteCommitCommitFunc,
	abort ClusterWriteCommitAbortFunc,
	recorder ClusterWriteCommitDecisionRecorder,
) (ClusterWriteCommitResult, error) {
	if recorder == nil {
		return ClusterWriteCommitResult{Proposal: proposal}, ErrClusterWriteCommitDecisionInvalid
	}
	return executeClusterWriteCommit(ctx, nodes, proposal, prepare, commit, abort, recorder)
}

func executeClusterWriteCommit(
	ctx context.Context,
	nodes []string,
	proposal ClusterWriteCommitProposal,
	prepare ClusterWriteCommitPrepareFunc,
	commit ClusterWriteCommitCommitFunc,
	abort ClusterWriteCommitAbortFunc,
	recorder ClusterWriteCommitDecisionRecorder,
) (ClusterWriteCommitResult, error) {
	proposal.TransactionID = strings.TrimSpace(proposal.TransactionID)
	result := ClusterWriteCommitResult{Proposal: proposal}
	if ctx == nil || proposal.TransactionID == "" || prepare == nil || commit == nil || abort == nil {
		return result, ErrClusterWriteCommitInvalid
	}
	normalizedNodes, err := normalizeClusterWriteCommitNodes(nodes)
	if err != nil {
		return result, err
	}
	result.Attempts = make([]ClusterWriteCommitAttempt, len(normalizedNodes))
	for index, node := range normalizedNodes {
		result.Attempts[index].Node = node
	}
	if err := ctx.Err(); err != nil {
		return result, errors.Join(ErrClusterWriteCommitPrepareFailed, err)
	}
	if err := recordClusterWriteCommitDecision(ctx, recorder, proposal, normalizedNodes, ClusterWriteCommitDecisionPreparing); err != nil {
		return result, err
	}

	var prepareGroup sync.WaitGroup
	prepareGroup.Add(len(result.Attempts))
	for index, node := range normalizedNodes {
		go func(index int, node string) {
			defer prepareGroup.Done()
			if err := prepare(ctx, node, proposal); err != nil {
				result.Attempts[index].PrepareError = err.Error()
				return
			}
			result.Attempts[index].Prepared = true
		}(index, node)
	}
	prepareGroup.Wait()

	prepareErrors := make([]error, 0)
	for index := range result.Attempts {
		attempt := &result.Attempts[index]
		if attempt.Prepared {
			result.PreparedCount++
			continue
		}
		if attempt.PrepareError != "" {
			prepareErrors = append(prepareErrors, fmt.Errorf("%s: %s", attempt.Node, attempt.PrepareError))
		}
	}
	if err := ctx.Err(); err != nil {
		prepareErrors = append(prepareErrors, err)
	}
	if len(prepareErrors) > 0 || result.PreparedCount != len(result.Attempts) {
		abortErrors := abortClusterWriteCommitPrepared(context.WithoutCancel(ctx), normalizedNodes, proposal, result.Attempts, abort)
		for index := range result.Attempts {
			if result.Attempts[index].Aborted {
				result.AbortedCount++
			}
		}
		if err := recordClusterWriteCommitDecisionAfterPhase(ctx, recorder, proposal, normalizedNodes, ClusterWriteCommitDecisionAborted); err != nil {
			prepareErrors = append(prepareErrors, err)
		}
		if len(abortErrors) > 0 {
			prepareErrors = append(prepareErrors, abortErrors...)
		}
		return result, errors.Join(append([]error{ErrClusterWriteCommitPrepareFailed}, prepareErrors...)...)
	}
	result.Prepared = true
	if err := recordClusterWriteCommitDecision(ctx, recorder, proposal, normalizedNodes, ClusterWriteCommitDecisionPrepared); err != nil {
		abortErrors := abortClusterWriteCommitPrepared(context.WithoutCancel(ctx), normalizedNodes, proposal, result.Attempts, abort)
		for index := range result.Attempts {
			if result.Attempts[index].Aborted {
				result.AbortedCount++
			}
		}
		if abortRecordErr := recordClusterWriteCommitDecisionAfterPhase(ctx, recorder, proposal, normalizedNodes, ClusterWriteCommitDecisionAborted); abortRecordErr != nil {
			abortErrors = append(abortErrors, abortRecordErr)
		}
		return result, errors.Join(append([]error{ErrClusterWriteCommitPrepareFailed, err}, abortErrors...)...)
	}
	if err := recordClusterWriteCommitDecision(ctx, recorder, proposal, normalizedNodes, ClusterWriteCommitDecisionCommitting); err != nil {
		abortErrors := abortClusterWriteCommitPrepared(context.WithoutCancel(ctx), normalizedNodes, proposal, result.Attempts, abort)
		for index := range result.Attempts {
			if result.Attempts[index].Aborted {
				result.AbortedCount++
			}
		}
		if abortRecordErr := recordClusterWriteCommitDecisionAfterPhase(ctx, recorder, proposal, normalizedNodes, ClusterWriteCommitDecisionAborted); abortRecordErr != nil {
			abortErrors = append(abortErrors, abortRecordErr)
		}
		return result, errors.Join(append([]error{ErrClusterWriteCommitDecisionPersist, err}, abortErrors...)...)
	}

	var commitGroup sync.WaitGroup
	commitGroup.Add(len(result.Attempts))
	for index, node := range normalizedNodes {
		go func(index int, node string) {
			defer commitGroup.Done()
			if err := commit(ctx, node, proposal); err != nil {
				result.Attempts[index].CommitError = err.Error()
				return
			}
			result.Attempts[index].Committed = true
		}(index, node)
	}
	commitGroup.Wait()

	commitErrors := make([]error, 0)
	for index := range result.Attempts {
		attempt := &result.Attempts[index]
		if attempt.Committed {
			result.CommittedCount++
			continue
		}
		if attempt.CommitError != "" {
			commitErrors = append(commitErrors, fmt.Errorf("%s: %s", attempt.Node, attempt.CommitError))
		}
	}
	if len(commitErrors) > 0 || result.CommittedCount != len(result.Attempts) {
		result.OutcomeUnknown = true
		if err := recordClusterWriteCommitDecisionAfterPhase(ctx, recorder, proposal, normalizedNodes, ClusterWriteCommitDecisionIndeterminate); err != nil {
			commitErrors = append(commitErrors, err)
		}
		return result, errors.Join(append([]error{ErrClusterWriteCommitOutcomeUnknown}, commitErrors...)...)
	}
	result.Committed = true
	if err := recordClusterWriteCommitDecisionAfterPhase(ctx, recorder, proposal, normalizedNodes, ClusterWriteCommitDecisionCommitted); err != nil {
		result.OutcomeUnknown = true
		return result, errors.Join(ErrClusterWriteCommitOutcomeUnknown, err)
	}
	return result, nil
}

func recordClusterWriteCommitDecision(
	ctx context.Context,
	recorder ClusterWriteCommitDecisionRecorder,
	proposal ClusterWriteCommitProposal,
	nodes []string,
	phase ClusterWriteCommitDecisionPhase,
) error {
	if recorder == nil {
		return nil
	}
	decision := ClusterWriteCommitDecision{
		Proposal: proposal,
		Nodes:    append([]string(nil), nodes...),
		Phase:    phase,
	}
	if err := recorder.Record(ctx, decision); err != nil {
		return errors.Join(ErrClusterWriteCommitDecisionPersist, err)
	}
	return nil
}

func recordClusterWriteCommitDecisionAfterPhase(
	ctx context.Context,
	recorder ClusterWriteCommitDecisionRecorder,
	proposal ClusterWriteCommitProposal,
	nodes []string,
	phase ClusterWriteCommitDecisionPhase,
) error {
	if recorder == nil {
		return nil
	}
	return recordClusterWriteCommitDecision(context.WithoutCancel(ctx), recorder, proposal, nodes, phase)
}

func normalizeClusterWriteCommitNodes(nodes []string) ([]string, error) {
	if len(nodes) == 0 || len(nodes) > MaxClusterWriteCommitNodes {
		return nil, fmt.Errorf("%w: node count %d", ErrClusterWriteCommitInvalid, len(nodes))
	}
	normalized := make([]string, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for index, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			return nil, fmt.Errorf("%w: node %d is blank", ErrClusterWriteCommitInvalid, index)
		}
		if _, ok := seen[node]; ok {
			return nil, fmt.Errorf("%w: duplicate node %q", ErrClusterWriteCommitInvalid, node)
		}
		seen[node] = struct{}{}
		normalized[index] = node
	}
	return normalized, nil
}

func abortClusterWriteCommitPrepared(
	ctx context.Context,
	nodes []string,
	proposal ClusterWriteCommitProposal,
	attempts []ClusterWriteCommitAttempt,
	abort ClusterWriteCommitAbortFunc,
) []error {
	var abortGroup sync.WaitGroup
	abortGroup.Add(len(attempts))
	errorsByIndex := make([]error, len(attempts))
	for index, node := range nodes {
		go func(index int, node string) {
			defer abortGroup.Done()
			if !attempts[index].Prepared {
				return
			}
			if err := abort(ctx, node, proposal); err != nil {
				attempts[index].AbortError = err.Error()
				errorsByIndex[index] = err
				return
			}
			attempts[index].Aborted = true
		}(index, node)
	}
	abortGroup.Wait()
	abortErrors := make([]error, 0)
	for index, err := range errorsByIndex {
		if err != nil {
			abortErrors = append(abortErrors, fmt.Errorf("%s: %w", nodes[index], err))
		}
	}
	return abortErrors
}
