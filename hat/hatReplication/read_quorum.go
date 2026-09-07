package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

var (
	// ErrReadQuorumExecutorInvalid reports invalid nodes, thresholds, context,
	// or callback input.
	ErrReadQuorumExecutorInvalid = errors.New("hatriecache: read quorum executor input is invalid")
	// ErrReadQuorumContextCanceled reports cancellation before or during reads.
	ErrReadQuorumContextCanceled = errors.New("hatriecache: read quorum execution was canceled")
	// ErrReadQuorumInconsistent reports successful reads without a matching
	// value at the requested threshold.
	ErrReadQuorumInconsistent = errors.New("hatriecache: read quorum values are inconsistent")
)

// ReadQuorumReadFunc reads one value from one named replica. The callback must
// be safe for concurrent calls and should honor ctx for bounded cancellation.
type ReadQuorumReadFunc func(context.Context, string) (any, error)

// ReadQuorumEqualFunc decides whether two successful replica values represent
// the same read result. A nil comparator uses reflect.DeepEqual.
type ReadQuorumEqualFunc func(left, right any) bool

// ReadQuorumAttempt records one replica callback outcome in input order.
type ReadQuorumAttempt struct {
	Node         string `json:"node"`
	Acknowledged bool   `json:"acknowledged"`
	Error        string `json:"error,omitempty"`
}

// ReadQuorumResult contains the matching value and deterministic per-replica
// outcomes. Decision.Acknowledged counts replicas matching Value, while each
// successful callback is represented by an acknowledged Attempt.
type ReadQuorumResult struct {
	Decision ReadQuorumDecision  `json:"decision"`
	Value    any                 `json:"value"`
	Attempts []ReadQuorumAttempt `json:"attempts"`
}

type readQuorumResponse struct {
	value any
	err   error
	ok    bool
}

type readQuorumGroup struct {
	value any
	count int
}

// ExecuteReadQuorum reads every supplied replica concurrently and returns the
// first input-order value group that reaches required acknowledgements. It is
// opt-in and does not change asynchronous replication or read-replica routing.
// Callers should provide a typed comparator for hot paths; nil is convenient
// for values where reflect.DeepEqual is an acceptable equality policy.
func ExecuteReadQuorum(ctx context.Context, nodes []string, required int, read ReadQuorumReadFunc, equal ReadQuorumEqualFunc) (ReadQuorumResult, error) {
	normalized, err := normalizeReadQuorumNodes(nodes, required, read)
	if err != nil {
		return ReadQuorumResult{}, err
	}
	if ctx == nil {
		return ReadQuorumResult{}, fmt.Errorf("%w: context is nil", ErrReadQuorumExecutorInvalid)
	}
	if err := ctx.Err(); err != nil {
		return ReadQuorumResult{}, fmt.Errorf("%w: %v", ErrReadQuorumContextCanceled, err)
	}
	if equal == nil {
		equal = reflect.DeepEqual
	}

	result := ReadQuorumResult{
		Decision: ReadQuorumDecision{Total: len(normalized), Required: required},
		Attempts: make([]ReadQuorumAttempt, len(normalized)),
	}
	responses := make([]readQuorumResponse, len(normalized))
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(normalized))
	for index, node := range normalized {
		result.Attempts[index].Node = node
		go func(index int, node string) {
			defer waitGroup.Done()
			value, err := read(ctx, node)
			if err != nil {
				responses[index].err = err
				result.Attempts[index].Error = err.Error()
				return
			}
			responses[index] = readQuorumResponse{value: value, ok: true}
			result.Attempts[index].Acknowledged = true
		}(index, node)
	}
	waitGroup.Wait()
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("%w: %v", ErrReadQuorumContextCanceled, err)
	}

	groups := make([]readQuorumGroup, 0, len(normalized))
	bestGroup := -1
	successful := 0
	for _, response := range responses {
		if !response.ok {
			continue
		}
		successful++
		groupIndex := -1
		for index := range groups {
			if equal(groups[index].value, response.value) {
				groupIndex = index
				break
			}
		}
		if groupIndex < 0 {
			groups = append(groups, readQuorumGroup{value: response.value, count: 1})
			groupIndex = len(groups) - 1
		} else {
			groups[groupIndex].count++
		}
		if bestGroup < 0 || groups[groupIndex].count > groups[bestGroup].count {
			bestGroup = groupIndex
		}
	}

	if bestGroup >= 0 {
		result.Decision.Acknowledged = groups[bestGroup].count
	}
	if successful < required {
		result.Decision.Acknowledged = successful
		return result, fmt.Errorf("%w: acknowledged=%d required=%d", ErrReadQuorumUnsatisfied, successful, required)
	}
	if result.Decision.Acknowledged < required {
		return result, fmt.Errorf("%w: matching=%d required=%d", ErrReadQuorumInconsistent, result.Decision.Acknowledged, required)
	}
	result.Decision.Satisfied = true
	result.Value = groups[bestGroup].value
	return result, nil
}

func normalizeReadQuorumNodes(nodes []string, required int, read ReadQuorumReadFunc) ([]string, error) {
	if len(nodes) == 0 || required < 1 || required > len(nodes) || read == nil {
		return nil, ErrReadQuorumExecutorInvalid
	}
	normalized := make([]string, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for index, node := range nodes {
		if node == "" {
			return nil, ErrReadQuorumExecutorInvalid
		}
		if _, found := seen[node]; found {
			return nil, ErrReadQuorumExecutorInvalid
		}
		seen[node] = struct{}{}
		normalized[index] = node
	}
	return normalized, nil
}
