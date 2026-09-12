package hatReplication

import (
	"context"
	"fmt"
)

// WriteQuorumEarlyResult reports the outcome of a quorum execution that may
// return before every target callback finishes. Attempts contains only
// completed callbacks, in the original target order. Pending counts callbacks
// that were still outstanding when the function returned; those callbacks
// receive a canceled context and must honor it for prompt shutdown.
type WriteQuorumEarlyResult struct {
	Decision WriteQuorumDecision  `json:"decision"`
	Attempts []WriteQuorumAttempt `json:"attempts"`
	Pending  int                  `json:"pending,omitempty"`
}

type writeQuorumEarlyResponse struct {
	index   int
	attempt WriteQuorumAttempt
}

// ExecuteWriteQuorumUntilSatisfied runs one write against every supplied
// target concurrently and returns as soon as the required acknowledgements are
// available or the quorum becomes impossible. It is opt-in and does not alter
// ExecuteWriteQuorum or the existing replication path. Targets that have not
// completed receive a canceled context after the early return; the write
// callback must honor context cancellation and remain safe to call concurrently.
func ExecuteWriteQuorumUntilSatisfied(ctx context.Context, nodes []string, required int, write WriteQuorumWriteFunc) (WriteQuorumEarlyResult, error) {
	normalized, err := normalizeWriteQuorumNodes(nodes, required, write)
	if err != nil {
		return WriteQuorumEarlyResult{}, err
	}
	result := WriteQuorumEarlyResult{
		Decision: WriteQuorumDecision{Total: len(normalized), Required: required},
	}
	if ctx == nil {
		return WriteQuorumEarlyResult{}, fmt.Errorf("%w: context is nil", ErrWriteQuorumExecutorInvalid)
	}
	if err := ctx.Err(); err != nil {
		result.Attempts = make([]WriteQuorumAttempt, len(normalized))
		for index, node := range normalized {
			result.Attempts[index] = WriteQuorumAttempt{Node: node, Error: err.Error()}
		}
		decision, _ := EvaluateWriteQuorum(result.Decision.Total, 0, required)
		result.Decision = decision
		return result, fmt.Errorf("%w: %v", ErrWriteQuorumContextCanceled, err)
	}

	workContext, cancel := context.WithCancel(ctx)
	defer cancel()
	responses := make(chan writeQuorumEarlyResponse, len(normalized))
	for index, node := range normalized {
		go func(index int, node string) {
			attempt := WriteQuorumAttempt{Node: node}
			if err := workContext.Err(); err != nil {
				attempt.Error = err.Error()
				responses <- writeQuorumEarlyResponse{index: index, attempt: attempt}
				return
			}
			if err := write(workContext, node); err != nil {
				attempt.Error = err.Error()
			} else {
				attempt.Acknowledged = true
			}
			responses <- writeQuorumEarlyResponse{index: index, attempt: attempt}
		}(index, node)
	}

	outcomes := make([]WriteQuorumAttempt, len(normalized))
	completed := make([]bool, len(normalized))
	completedCount := 0
	acknowledged := 0
	for completedCount < len(normalized) {
		response := <-responses
		if completed[response.index] {
			continue
		}
		completed[response.index] = true
		outcomes[response.index] = response.attempt
		completedCount++
		if response.attempt.Acknowledged {
			acknowledged++
		}

		remaining := len(normalized) - completedCount
		if acknowledged >= required || acknowledged+remaining < required {
			decision, decisionErr := EvaluateWriteQuorum(len(normalized), acknowledged, required)
			result.Decision = decision
			result.Attempts = collectWriteQuorumAttempts(outcomes, completed)
			result.Pending = remaining
			cancel()
			if decisionErr != nil {
				return result, decisionErr
			}
			return result, nil
		}
	}

	decision, decisionErr := EvaluateWriteQuorum(len(normalized), acknowledged, required)
	result.Decision = decision
	result.Attempts = collectWriteQuorumAttempts(outcomes, completed)
	if decisionErr != nil {
		return result, decisionErr
	}
	return result, nil
}

func collectWriteQuorumAttempts(outcomes []WriteQuorumAttempt, completed []bool) []WriteQuorumAttempt {
	attempts := make([]WriteQuorumAttempt, 0, len(outcomes))
	for index, attempt := range outcomes {
		if completed[index] {
			attempts = append(attempts, attempt)
		}
	}
	return attempts
}
