package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrIncrementalRecursiveReachabilityNil           = errors.New("hatSql: incremental recursive reachability is nil")
	ErrIncrementalRecursiveReachabilityEdgeRequired  = errors.New("hatSql: recursive reachability edge key is required")
	ErrIncrementalRecursiveReachabilityNodeRequired  = errors.New("hatSql: recursive reachability node is required")
	ErrIncrementalRecursiveReachabilityDuplicateEdge = errors.New("hatSql: recursive reachability edge key already exists")
)

// RecursiveReachabilityEdge is one append-only directed edge. Key is the
// stable source identity of the edge and must be unique for the lifetime of
// the maintainer.
type RecursiveReachabilityEdge struct {
	Key  string
	From string
	To   string
}

// IncrementalRecursiveReachability maintains the positive transitive closure
// of an append-only graph. It is the monotone portion of a recursive
// differential dataflow: Append returns only newly discovered reachable pairs
// as positive DifferentialRow updates.
//
// Deletes and edge updates are intentionally unsupported. A caller needing
// those operations should rebuild the closure from the current edge set.
type IncrementalRecursiveReachability struct {
	mu           sync.RWMutex
	edges        map[string]struct{}
	reachable    map[string]map[string]struct{}
	ancestors    map[string]map[string]struct{}
	mutableEdges map[string]RecursiveReachabilityEdge
}

// NewIncrementalRecursiveReachability creates an empty append-only recursive
// reachability maintainer.
func NewIncrementalRecursiveReachability() *IncrementalRecursiveReachability {
	return &IncrementalRecursiveReachability{
		edges:     make(map[string]struct{}),
		reachable: make(map[string]map[string]struct{}),
		ancestors: make(map[string]map[string]struct{}),
	}
}

// Append adds edges atomically and returns newly discovered transitive pairs.
// The returned row key is a NUL-delimited source/target pair, and each row has
// the fields "from" and "to". Input validation happens before state changes.
func (reachability *IncrementalRecursiveReachability) Append(edges []RecursiveReachabilityEdge) ([]DifferentialRow, error) {
	if reachability == nil {
		return nil, ErrIncrementalRecursiveReachabilityNil
	}
	if len(edges) == 0 {
		return nil, nil
	}

	reachability.mu.Lock()
	defer reachability.mu.Unlock()
	return reachability.appendLocked(edges)
}

func (reachability *IncrementalRecursiveReachability) appendLocked(edges []RecursiveReachabilityEdge) ([]DifferentialRow, error) {

	pending := make(map[string]struct{}, len(edges))
	var pendingEdges map[string]RecursiveReachabilityEdge
	if reachability.mutableEdges != nil {
		pendingEdges = make(map[string]RecursiveReachabilityEdge, len(edges))
	}
	for _, edge := range edges {
		if err := validateRecursiveReachabilityEdge(edge); err != nil {
			return nil, err
		}
		if _, exists := reachability.edges[edge.Key]; exists {
			return nil, fmt.Errorf("%w: %q", ErrIncrementalRecursiveReachabilityDuplicateEdge, edge.Key)
		}
		if _, exists := pending[edge.Key]; exists {
			return nil, fmt.Errorf("%w: %q", ErrIncrementalRecursiveReachabilityDuplicateEdge, edge.Key)
		}
		pending[edge.Key] = struct{}{}
	}

	updates := make([]DifferentialRow, 0, len(edges))
	for _, edge := range edges {
		reachability.edges[edge.Key] = struct{}{}
		if pendingEdges != nil {
			pendingEdges[edge.Key] = edge
		}
		sources := recursiveReachabilityEndpoints(reachability.ancestors[edge.From], edge.From)
		destinations := recursiveReachabilityEndpoints(reachability.reachable[edge.To], edge.To)
		for _, source := range sources {
			for _, destination := range destinations {
				if _, exists := reachability.reachable[source][destination]; exists {
					continue
				}
				if reachability.reachable[source] == nil {
					reachability.reachable[source] = make(map[string]struct{})
				}
				reachability.reachable[source][destination] = struct{}{}
				if reachability.ancestors[destination] == nil {
					reachability.ancestors[destination] = make(map[string]struct{})
				}
				reachability.ancestors[destination][source] = struct{}{}
				updates = append(updates, DifferentialRow{
					Key:  recursiveReachabilityPairKey(source, destination),
					Diff: 1,
					Row: Row{
						"from": source,
						"to":   destination,
					},
				})
			}
		}
	}
	if pendingEdges != nil {
		if reachability.mutableEdges == nil {
			reachability.mutableEdges = make(map[string]RecursiveReachabilityEdge, len(pendingEdges))
		}
		for key, edge := range pendingEdges {
			reachability.mutableEdges[key] = edge
		}
	}
	if len(updates) == 0 {
		return nil, nil
	}
	return updates, nil
}

// Reachable reports whether at least one directed path exists from from to
// to. It is safe to call concurrently with Append.
func (reachability *IncrementalRecursiveReachability) Reachable(from, to string) bool {
	if reachability == nil {
		return false
	}
	reachability.mu.RLock()
	defer reachability.mu.RUnlock()
	_, reachable := reachability.reachable[from][to]
	return reachable
}

func validateRecursiveReachabilityEdge(edge RecursiveReachabilityEdge) error {
	if strings.TrimSpace(edge.Key) == "" {
		return ErrIncrementalRecursiveReachabilityEdgeRequired
	}
	if err := validateRecursiveReachabilityNode(edge.From); err != nil {
		return fmt.Errorf("recursive reachability edge %q source: %w", edge.Key, err)
	}
	if err := validateRecursiveReachabilityNode(edge.To); err != nil {
		return fmt.Errorf("recursive reachability edge %q target: %w", edge.Key, err)
	}
	return nil
}

func validateRecursiveReachabilityNode(node string) error {
	if strings.TrimSpace(node) == "" || strings.IndexByte(node, 0) >= 0 {
		return ErrIncrementalRecursiveReachabilityNodeRequired
	}
	return nil
}

func recursiveReachabilityEndpoints(existing map[string]struct{}, own string) []string {
	endpoints := make([]string, 0, len(existing)+1)
	endpoints = append(endpoints, own)
	for endpoint := range existing {
		if endpoint != own {
			endpoints = append(endpoints, endpoint)
		}
	}
	sort.Strings(endpoints)
	return endpoints
}

func recursiveReachabilityPairKey(from, to string) string {
	return from + "\x00" + to
}
