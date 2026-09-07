package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrWriteQuorumExecutorInvalid reports invalid target, threshold, context,
	// or callback input.
	ErrWriteQuorumExecutorInvalid = errors.New("hatriecache: write quorum executor input is invalid")
	// ErrWriteQuorumContextCanceled reports cancellation before the quorum
	// executor could run its target callbacks.
	ErrWriteQuorumContextCanceled = errors.New("hatriecache: write quorum execution was canceled")
)

// WriteQuorumWriteFunc applies one write to one named replica. The callback
// must be safe for concurrent calls because targets are attempted in parallel.
type WriteQuorumWriteFunc func(context.Context, string) error

// WriteQuorumAttempt records the outcome for one target in the same order as
// the target list passed to ExecuteWriteQuorum.
type WriteQuorumAttempt struct {
	Node         string `json:"node"`
	Acknowledged bool   `json:"acknowledged"`
	Error        string `json:"error,omitempty"`
}

// WriteQuorumResult combines the explicit acknowledgement decision with every
// target outcome. All supplied targets are attempted so callers can repair
// failed replicas after a satisfied quorum.
type WriteQuorumResult struct {
	Decision WriteQuorumDecision  `json:"decision"`
	Attempts []WriteQuorumAttempt `json:"attempts"`
}

// ExecuteWriteQuorum runs one write against every supplied target concurrently
// and requires required successful acknowledgements. The operation is opt-in;
// it does not alter the existing asynchronous replication path. A satisfied
// quorum does not roll back failed targets, so callers should use the returned
// attempts for repair or later reconciliation.
func ExecuteWriteQuorum(ctx context.Context, nodes []string, required int, write WriteQuorumWriteFunc) (WriteQuorumResult, error) {
	normalized, err := normalizeWriteQuorumNodes(nodes, required, write)
	if err != nil {
		return WriteQuorumResult{}, err
	}
	result := WriteQuorumResult{
		Decision: WriteQuorumDecision{Total: len(normalized), Required: required},
		Attempts: make([]WriteQuorumAttempt, len(normalized)),
	}
	for index, node := range normalized {
		result.Attempts[index].Node = node
	}
	if ctx == nil {
		return WriteQuorumResult{}, fmt.Errorf("%w: context is nil", ErrWriteQuorumExecutorInvalid)
	}
	if ctx.Err() != nil {
		for index := range result.Attempts {
			result.Attempts[index].Error = ctx.Err().Error()
		}
		return result, fmt.Errorf("%w: %v", ErrWriteQuorumContextCanceled, ctx.Err())
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(normalized))
	for index, node := range normalized {
		go func(index int, node string) {
			defer waitGroup.Done()
			if err := ctx.Err(); err != nil {
				result.Attempts[index].Error = err.Error()
				return
			}
			if err := write(ctx, node); err != nil {
				result.Attempts[index].Error = err.Error()
				return
			}
			result.Attempts[index].Acknowledged = true
		}(index, node)
	}
	waitGroup.Wait()

	for _, attempt := range result.Attempts {
		if attempt.Acknowledged {
			result.Decision.Acknowledged++
		}
	}
	decision, decisionErr := EvaluateWriteQuorum(result.Decision.Total, result.Decision.Acknowledged, result.Decision.Required)
	result.Decision = decision
	if decisionErr != nil {
		return result, decisionErr
	}
	return result, nil
}

func normalizeWriteQuorumNodes(nodes []string, required int, write WriteQuorumWriteFunc) ([]string, error) {
	if len(nodes) == 0 || required < 1 || required > len(nodes) || write == nil {
		return nil, ErrWriteQuorumExecutorInvalid
	}
	normalized := make([]string, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for index, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			return nil, ErrWriteQuorumExecutorInvalid
		}
		if _, found := seen[node]; found {
			return nil, ErrWriteQuorumExecutorInvalid
		}
		seen[node] = struct{}{}
		normalized[index] = node
	}
	return normalized, nil
}
