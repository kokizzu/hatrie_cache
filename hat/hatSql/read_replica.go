package hatSql

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

const maxReadReplicaAttempts = 8

var ErrReadReplicaRetryOptionsInvalid = errors.New("hatSql: read replica retry options are invalid")

// ReadReplicaSet distributes read-only query execution across source resolvers.
type ReadReplicaSet struct {
	replicas []SourceResolver
	next     atomic.Uint64
}

// ReadReplicaRetryOptions bounds retries for ExecuteWithRetry. MaxAttempts
// defaults to one, preserving the ordinary single-read behavior. A classifier
// is required when retries are enabled so callers do not repeat non-transient
// queries accidentally. Backoff, when provided, receives the failed attempt
// number starting at one.
type ReadReplicaRetryOptions struct {
	MaxAttempts int
	Retryable   func(error) bool
	Backoff     func(attempt int) time.Duration
}

// NewReadReplicaSet constructs a round-robin SQL read router.
func NewReadReplicaSet(replicas ...SourceResolver) (*ReadReplicaSet, error) {
	if len(replicas) == 0 {
		return nil, errors.New("hatSql: at least one SQL read replica is required")
	}
	out := make([]SourceResolver, 0, len(replicas))
	for _, replica := range replicas {
		if replica == nil {
			return nil, errors.New("hatSql: SQL read replica is nil")
		}
		out = append(out, replica)
	}
	return &ReadReplicaSet{replicas: out}, nil
}

// Execute runs a query against the next resolver in stable round-robin order.
func (set *ReadReplicaSet) Execute(ctx context.Context, source string, parameters []interface{}, options QueryOptions) (QueryResult, error) {
	if set == nil || len(set.replicas) == 0 {
		return QueryResult{}, errors.New("hatSql: SQL read replica set is empty")
	}
	index := set.next.Add(1) - 1
	resolver := set.replicas[index%uint64(len(set.replicas))]
	return ExecuteQueryParameters(ctx, source, resolver, parameters, options)
}

// ExecuteWithRetry retries a failed read on subsequent replicas when the
// caller classifies the error as retryable. The hard attempt cap prevents an
// accidental retry loop and each backoff wait is cancelable by ctx.
func (set *ReadReplicaSet) ExecuteWithRetry(ctx context.Context, source string, parameters []interface{}, options QueryOptions, retry ReadReplicaRetryOptions) (QueryResult, error) {
	attempts, err := normalizeReadReplicaRetryOptions(retry)
	if err != nil {
		return QueryResult{}, err
	}
	var result QueryResult
	for attempt := 1; attempt <= attempts; attempt++ {
		result, err = set.Execute(ctx, source, parameters, options)
		if err == nil {
			return result, nil
		}
		if attempt == attempts || retry.Retryable == nil || !retry.Retryable(err) {
			return result, err
		}
		delay := time.Duration(0)
		if retry.Backoff != nil {
			delay = retry.Backoff(attempt)
			if delay < 0 {
				return QueryResult{}, ErrReadReplicaRetryOptionsInvalid
			}
		}
		if err := waitReadReplicaRetry(ctx, delay); err != nil {
			return QueryResult{}, err
		}
	}
	return result, err
}

// ExecuteSQLQuery is retained for callers that use the root API naming.
func (set *ReadReplicaSet) ExecuteSQLQuery(ctx context.Context, source string, parameters []interface{}, options QueryOptions) (QueryResult, error) {
	return set.Execute(ctx, source, parameters, options)
}

// ExecuteSQLQueryWithRetry is the root-API-named alias for ExecuteWithRetry.
func (set *ReadReplicaSet) ExecuteSQLQueryWithRetry(ctx context.Context, source string, parameters []interface{}, options QueryOptions, retry ReadReplicaRetryOptions) (QueryResult, error) {
	return set.ExecuteWithRetry(ctx, source, parameters, options, retry)
}

func normalizeReadReplicaRetryOptions(options ReadReplicaRetryOptions) (int, error) {
	attempts := options.MaxAttempts
	if attempts == 0 {
		attempts = 1
	}
	if attempts < 1 || attempts > maxReadReplicaAttempts {
		return 0, ErrReadReplicaRetryOptionsInvalid
	}
	if attempts > 1 && options.Retryable == nil {
		return 0, ErrReadReplicaRetryOptionsInvalid
	}
	return attempts, nil
}

func waitReadReplicaRetry(ctx context.Context, delay time.Duration) error {
	if delay == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
