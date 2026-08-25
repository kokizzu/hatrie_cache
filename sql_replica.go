package hatriecache

import (
	"context"
	"errors"
	"sync/atomic"
)

// SQLReadReplicaSet distributes read-only SQL query execution across replicas.
// Replication remains the responsibility of the normal command replicator; the
// set deliberately has no mutation API.
type SQLReadReplicaSet struct {
	replicas []SQLSourceResolver
	next     atomic.Uint64
}

// NewSQLReadReplicaSet creates a search-read router from one or more SQL
// source resolvers. *HatTrie values satisfy this interface directly.
func NewSQLReadReplicaSet(replicas ...SQLSourceResolver) (*SQLReadReplicaSet, error) {
	if len(replicas) == 0 {
		return nil, errors.New("hatriecache: at least one SQL read replica is required")
	}
	out := make([]SQLSourceResolver, 0, len(replicas))
	for _, replica := range replicas {
		if replica == nil {
			return nil, errors.New("hatriecache: SQL read replica is nil")
		}
		out = append(out, replica)
	}
	return &SQLReadReplicaSet{replicas: out}, nil
}

// ExecuteSQLQuery runs one read query against the next replica in stable
// round-robin order. It uses the standard query executor, so indexes, query
// limits, cancellation, and observer behavior are preserved.
func (set *SQLReadReplicaSet) ExecuteSQLQuery(ctx context.Context, source string, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	if set == nil || len(set.replicas) == 0 {
		return SQLQueryResult{}, errors.New("hatriecache: SQL read replica set is empty")
	}
	index := set.next.Add(1) - 1
	resolver := set.replicas[index%uint64(len(set.replicas))]
	return ExecuteSQLQueryParameters(ctx, source, resolver, parameters, options)
}
