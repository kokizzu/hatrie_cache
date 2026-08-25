package hatSql

import (
	"context"
	"errors"
	"sync/atomic"
)

// ReadReplicaSet distributes read-only query execution across source resolvers.
type ReadReplicaSet struct {
	replicas []SourceResolver
	next     atomic.Uint64
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

// ExecuteSQLQuery is retained for callers that use the root API naming.
func (set *ReadReplicaSet) ExecuteSQLQuery(ctx context.Context, source string, parameters []interface{}, options QueryOptions) (QueryResult, error) {
	return set.Execute(ctx, source, parameters, options)
}
