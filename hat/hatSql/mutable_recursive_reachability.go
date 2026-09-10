package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	// ErrIncrementalRecursiveReachabilityMutationsDisabled reports use of
	// Apply on an append-only reachability maintainer.
	ErrIncrementalRecursiveReachabilityMutationsDisabled = errors.New("hatSql: recursive reachability mutations are disabled")
	// ErrIncrementalRecursiveReachabilityMutationInvalid reports an unsupported
	// mutation kind.
	ErrIncrementalRecursiveReachabilityMutationInvalid = errors.New("hatSql: recursive reachability mutation kind is invalid")
	// ErrIncrementalRecursiveReachabilityMutationKeyRequired reports a missing
	// mutation identity.
	ErrIncrementalRecursiveReachabilityMutationKeyRequired = errors.New("hatSql: recursive reachability mutation key is required")
	// ErrIncrementalRecursiveReachabilityMutationDuplicate reports a repeated
	// mutation identity or an insert of an existing edge.
	ErrIncrementalRecursiveReachabilityMutationDuplicate = errors.New("hatSql: recursive reachability mutation key is duplicated")
	// ErrIncrementalRecursiveReachabilityMutationMissing reports an update or
	// delete for an unknown edge.
	ErrIncrementalRecursiveReachabilityMutationMissing = errors.New("hatSql: recursive reachability mutation key is missing")
)

// RecursiveReachabilityMutationKind identifies an edge operation.
type RecursiveReachabilityMutationKind string

const (
	RecursiveReachabilityInsert RecursiveReachabilityMutationKind = "INSERT"
	RecursiveReachabilityUpdate RecursiveReachabilityMutationKind = "UPDATE"
	RecursiveReachabilityDelete RecursiveReachabilityMutationKind = "DELETE"
)

// RecursiveReachabilityMutation changes one stable edge identity. DELETE uses
// only Key; INSERT and UPDATE validate From and To as graph nodes.
type RecursiveReachabilityMutation struct {
	Kind RecursiveReachabilityMutationKind
	Key  string
	From string
	To   string
}

// NewMutableIncrementalRecursiveReachability creates a reachability
// maintainer that retains edge endpoints and supports exact edge mutations.
// NewIncrementalRecursiveReachability remains append-only and does not retain
// this extra edge metadata.
func NewMutableIncrementalRecursiveReachability() *IncrementalRecursiveReachability {
	reachability := NewIncrementalRecursiveReachability()
	reachability.mutableEdges = make(map[string]RecursiveReachabilityEdge)
	return reachability
}

// Apply atomically applies edge inserts, updates, and deletes. Insert-only
// batches use the monotone append path. Other batches recompute only source
// nodes whose paths can cross a changed edge, then emit signed pair changes.
func (reachability *IncrementalRecursiveReachability) Apply(mutations []RecursiveReachabilityMutation) ([]DifferentialRow, error) {
	if reachability == nil {
		return nil, ErrIncrementalRecursiveReachabilityNil
	}
	if reachability.mutableEdges == nil {
		return nil, ErrIncrementalRecursiveReachabilityMutationsDisabled
	}
	if len(mutations) == 0 {
		return nil, nil
	}

	reachability.mu.Lock()
	defer reachability.mu.Unlock()

	normalized := make([]RecursiveReachabilityMutation, len(mutations))
	seen := make(map[string]struct{}, len(mutations))
	allInserts := true
	for index, mutation := range mutations {
		kind := RecursiveReachabilityMutationKind(strings.ToUpper(strings.TrimSpace(string(mutation.Kind))))
		if kind != RecursiveReachabilityInsert && kind != RecursiveReachabilityUpdate && kind != RecursiveReachabilityDelete {
			return nil, fmt.Errorf("recursive reachability mutation %d: %w", index, ErrIncrementalRecursiveReachabilityMutationInvalid)
		}
		key := mutation.Key
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("recursive reachability mutation %d: %w", index, ErrIncrementalRecursiveReachabilityMutationKeyRequired)
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("recursive reachability mutation %d key %q: %w", index, key, ErrIncrementalRecursiveReachabilityMutationDuplicate)
		}
		seen[key] = struct{}{}
		_, exists := reachability.mutableEdges[key]
		switch kind {
		case RecursiveReachabilityInsert:
			if exists {
				return nil, fmt.Errorf("recursive reachability mutation %d key %q: %w", index, key, ErrIncrementalRecursiveReachabilityMutationDuplicate)
			}
			edge := RecursiveReachabilityEdge{Key: key, From: mutation.From, To: mutation.To}
			if err := validateRecursiveReachabilityEdge(edge); err != nil {
				return nil, fmt.Errorf("recursive reachability mutation %d: %w", index, err)
			}
		case RecursiveReachabilityUpdate:
			allInserts = false
			if !exists {
				return nil, fmt.Errorf("recursive reachability mutation %d key %q: %w", index, key, ErrIncrementalRecursiveReachabilityMutationMissing)
			}
			edge := RecursiveReachabilityEdge{Key: key, From: mutation.From, To: mutation.To}
			if err := validateRecursiveReachabilityEdge(edge); err != nil {
				return nil, fmt.Errorf("recursive reachability mutation %d: %w", index, err)
			}
		case RecursiveReachabilityDelete:
			allInserts = false
			if !exists {
				return nil, fmt.Errorf("recursive reachability mutation %d key %q: %w", index, key, ErrIncrementalRecursiveReachabilityMutationMissing)
			}
		}
		normalized[index] = RecursiveReachabilityMutation{Kind: kind, Key: key, From: mutation.From, To: mutation.To}
	}

	if allInserts {
		edges := make([]RecursiveReachabilityEdge, len(normalized))
		for index, mutation := range normalized {
			edges[index] = RecursiveReachabilityEdge{Key: mutation.Key, From: mutation.From, To: mutation.To}
		}
		return reachability.appendLocked(edges)
	}
	if len(normalized) == 1 && normalized[0].Kind == RecursiveReachabilityDelete {
		if updates, applied := reachability.tryLeafDeleteLocked(normalized[0], nil); applied {
			return updates, nil
		}
	}
	if len(normalized) == 1 && normalized[0].Kind == RecursiveReachabilityUpdate {
		if updates, applied := reachability.tryLeafUpdateLocked(normalized[0]); applied {
			return updates, nil
		}
	}
	candidate := make(map[string]RecursiveReachabilityEdge, len(reachability.mutableEdges)+len(mutations))
	for key, edge := range reachability.mutableEdges {
		candidate[key] = edge
	}
	for _, mutation := range normalized {
		switch mutation.Kind {
		case RecursiveReachabilityInsert, RecursiveReachabilityUpdate:
			candidate[mutation.Key] = RecursiveReachabilityEdge{Key: mutation.Key, From: mutation.From, To: mutation.To}
		case RecursiveReachabilityDelete:
			delete(candidate, mutation.Key)
		}
	}
	return reachability.applyMutableLocked(normalized, candidate)
}

func (reachability *IncrementalRecursiveReachability) applyMutableLocked(mutations []RecursiveReachabilityMutation, candidate map[string]RecursiveReachabilityEdge) ([]DifferentialRow, error) {
	if len(mutations) == 1 && mutations[0].Kind == RecursiveReachabilityDelete {
		if updates, applied := reachability.tryLeafDeleteLocked(mutations[0], candidate); applied {
			return updates, nil
		}
	}
	adjacency, reverse := buildRecursiveReachabilityGraph(candidate)
	affectedSources := make(map[string]struct{})
	for _, mutation := range mutations {
		if mutation.Kind == RecursiveReachabilityUpdate || mutation.Kind == RecursiveReachabilityDelete {
			oldEdge := reachability.mutableEdges[mutation.Key]
			affectedSources[oldEdge.From] = struct{}{}
			for source := range reachability.ancestors[oldEdge.From] {
				affectedSources[source] = struct{}{}
			}
			for source := range recursiveReachabilityReverseSources(oldEdge.From, reverse) {
				affectedSources[source] = struct{}{}
			}
		}
		if mutation.Kind == RecursiveReachabilityInsert || mutation.Kind == RecursiveReachabilityUpdate {
			for source := range recursiveReachabilityReverseSources(mutation.From, reverse) {
				affectedSources[source] = struct{}{}
			}
		}
	}

	nextReachable := make(map[string]map[string]struct{}, len(reachability.reachable)+len(affectedSources))
	for source, destinations := range reachability.reachable {
		nextReachable[source] = destinations
	}
	for source := range affectedSources {
		destinations := recursiveReachabilityFrom(source, adjacency)
		if len(destinations) == 0 {
			delete(nextReachable, source)
			continue
		}
		nextReachable[source] = destinations
	}

	changes := collectRecursiveReachabilityChanges(reachability.reachable, nextReachable, affectedSources)
	nextAncestors := cloneRecursiveReachabilityAncestors(reachability.ancestors)
	clonedAncestors := make(map[string]struct{})
	updates := make([]DifferentialRow, 0, len(changes)*2)
	for _, change := range changes {
		ancestors := nextAncestors[change.to]
		if _, cloned := clonedAncestors[change.to]; !cloned || ancestors == nil {
			copyAncestors := make(map[string]struct{}, len(ancestors)+1)
			for source := range ancestors {
				copyAncestors[source] = struct{}{}
			}
			ancestors = copyAncestors
			nextAncestors[change.to] = ancestors
			clonedAncestors[change.to] = struct{}{}
		}
		if change.present {
			ancestors[change.from] = struct{}{}
			updates = append(updates, DifferentialRow{
				Key:  recursiveReachabilityPairKey(change.from, change.to),
				Diff: 1,
				Row:  Row{"from": change.from, "to": change.to},
			})
			continue
		}
		delete(ancestors, change.from)
		if len(ancestors) == 0 {
			delete(nextAncestors, change.to)
		}
		updates = append(updates, DifferentialRow{
			Key:  recursiveReachabilityPairKey(change.from, change.to),
			Diff: -1,
			Row:  Row{"from": change.from, "to": change.to},
		})
	}

	nextEdges := make(map[string]struct{}, len(candidate))
	for key := range candidate {
		nextEdges[key] = struct{}{}
	}
	reachability.edges = nextEdges
	reachability.mutableEdges = candidate
	reachability.reachable = nextReachable
	reachability.ancestors = nextAncestors
	return updates, nil
}

func (reachability *IncrementalRecursiveReachability) tryLeafDeleteLocked(mutation RecursiveReachabilityMutation, candidate map[string]RecursiveReachabilityEdge) ([]DifferentialRow, bool) {
	oldEdge := reachability.mutableEdges[mutation.Key]
	if candidate == nil {
		for key, edge := range reachability.mutableEdges {
			if key == mutation.Key {
				continue
			}
			if edge.To == oldEdge.To || edge.From == oldEdge.To {
				return nil, false
			}
		}
	} else {
		for _, edge := range candidate {
			if edge.To == oldEdge.To || edge.From == oldEdge.To {
				return nil, false
			}
		}
	}

	sources := make(map[string]struct{}, len(reachability.ancestors[oldEdge.From])+1)
	sources[oldEdge.From] = struct{}{}
	for source := range reachability.ancestors[oldEdge.From] {
		sources[source] = struct{}{}
	}
	orderedSources := make([]string, 0, len(sources))
	for source := range sources {
		orderedSources = append(orderedSources, source)
	}
	sort.Strings(orderedSources)
	updates := make([]DifferentialRow, 0, len(orderedSources))
	for _, source := range orderedSources {
		destinations := reachability.reachable[source]
		if _, exists := destinations[oldEdge.To]; !exists {
			continue
		}
		delete(destinations, oldEdge.To)
		if len(destinations) == 0 {
			delete(reachability.reachable, source)
		}
		updates = append(updates, DifferentialRow{
			Key:  recursiveReachabilityPairKey(source, oldEdge.To),
			Diff: -1,
			Row:  Row{"from": source, "to": oldEdge.To},
		})
	}
	if destinations := reachability.ancestors[oldEdge.To]; destinations != nil {
		for source := range sources {
			delete(destinations, source)
		}
		if len(destinations) == 0 {
			delete(reachability.ancestors, oldEdge.To)
		}
	}
	delete(reachability.edges, mutation.Key)
	if candidate == nil {
		delete(reachability.mutableEdges, mutation.Key)
	} else {
		reachability.mutableEdges = candidate
	}
	return updates, true
}

func (reachability *IncrementalRecursiveReachability) tryLeafUpdateLocked(mutation RecursiveReachabilityMutation) ([]DifferentialRow, bool) {
	oldEdge := reachability.mutableEdges[mutation.Key]
	if oldEdge.From != mutation.From || oldEdge.To == mutation.To || oldEdge.From == oldEdge.To || mutation.From == mutation.To {
		return nil, false
	}
	for key, edge := range reachability.mutableEdges {
		if key == mutation.Key {
			continue
		}
		if edge.To == oldEdge.To || edge.From == oldEdge.To || edge.To == mutation.To || edge.From == mutation.To {
			return nil, false
		}
	}

	sources := make(map[string]struct{}, len(reachability.ancestors[oldEdge.From])+1)
	sources[oldEdge.From] = struct{}{}
	for source := range reachability.ancestors[oldEdge.From] {
		sources[source] = struct{}{}
	}
	orderedSources := make([]string, 0, len(sources))
	for source := range sources {
		if _, exists := reachability.reachable[source][oldEdge.To]; !exists {
			continue
		}
		orderedSources = append(orderedSources, source)
	}
	if len(orderedSources) == 0 {
		return nil, false
	}
	sort.Strings(orderedSources)
	newAncestors := reachability.ancestors[mutation.To]
	if newAncestors == nil {
		newAncestors = make(map[string]struct{}, len(orderedSources))
		reachability.ancestors[mutation.To] = newAncestors
	}
	oldAncestors := reachability.ancestors[oldEdge.To]
	for _, source := range orderedSources {
		destinations := reachability.reachable[source]
		delete(destinations, oldEdge.To)
		destinations[mutation.To] = struct{}{}
		delete(oldAncestors, source)
		newAncestors[source] = struct{}{}
	}
	if len(oldAncestors) == 0 {
		delete(reachability.ancestors, oldEdge.To)
	}
	reachability.mutableEdges[mutation.Key] = RecursiveReachabilityEdge{Key: mutation.Key, From: mutation.From, To: mutation.To}
	changes := make([]recursiveReachabilityChange, 0, len(orderedSources)*2)
	for _, source := range orderedSources {
		changes = append(changes,
			recursiveReachabilityChange{from: source, to: oldEdge.To, present: false},
			recursiveReachabilityChange{from: source, to: mutation.To, present: true},
		)
	}
	sort.Slice(changes, func(left, right int) bool {
		leftKey := recursiveReachabilityPairKey(changes[left].from, changes[left].to)
		rightKey := recursiveReachabilityPairKey(changes[right].from, changes[right].to)
		return leftKey < rightKey
	})
	updates := make([]DifferentialRow, 0, len(changes))
	for _, change := range changes {
		diff := int64(-1)
		if change.present {
			diff = 1
		}
		updates = append(updates, DifferentialRow{
			Key:  recursiveReachabilityPairKey(change.from, change.to),
			Diff: diff,
			Row:  Row{"from": change.from, "to": change.to},
		})
	}
	return updates, true
}

type recursiveReachabilityChange struct {
	from    string
	to      string
	present bool
}

func collectRecursiveReachabilityChanges(oldReachable, newReachable map[string]map[string]struct{}, affectedSources map[string]struct{}) []recursiveReachabilityChange {
	changes := make([]recursiveReachabilityChange, 0)
	for source := range affectedSources {
		destinations := make(map[string]struct{}, len(oldReachable[source])+len(newReachable[source]))
		for destination := range oldReachable[source] {
			destinations[destination] = struct{}{}
		}
		for destination := range newReachable[source] {
			destinations[destination] = struct{}{}
		}
		for destination := range destinations {
			_, wasPresent := oldReachable[source][destination]
			_, isPresent := newReachable[source][destination]
			if wasPresent != isPresent {
				changes = append(changes, recursiveReachabilityChange{from: source, to: destination, present: isPresent})
			}
		}
	}
	sort.Slice(changes, func(left, right int) bool {
		leftKey := recursiveReachabilityPairKey(changes[left].from, changes[left].to)
		rightKey := recursiveReachabilityPairKey(changes[right].from, changes[right].to)
		return leftKey < rightKey
	})
	return changes
}

func buildRecursiveReachabilityGraph(edges map[string]RecursiveReachabilityEdge) (map[string][]string, map[string][]string) {
	adjacency := make(map[string][]string)
	reverse := make(map[string][]string)
	for _, edge := range edges {
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
		reverse[edge.To] = append(reverse[edge.To], edge.From)
	}
	for node := range adjacency {
		sort.Strings(adjacency[node])
	}
	for node := range reverse {
		sort.Strings(reverse[node])
	}
	return adjacency, reverse
}

func recursiveReachabilityReverseSources(start string, reverse map[string][]string) map[string]struct{} {
	sources := map[string]struct{}{start: {}}
	stack := []string{start}
	for len(stack) > 0 {
		last := len(stack) - 1
		node := stack[last]
		stack = stack[:last]
		for _, source := range reverse[node] {
			if _, exists := sources[source]; exists {
				continue
			}
			sources[source] = struct{}{}
			stack = append(stack, source)
		}
	}
	return sources
}

func recursiveReachabilityFrom(source string, adjacency map[string][]string) map[string]struct{} {
	if len(adjacency[source]) == 0 {
		return nil
	}
	visited := map[string]struct{}{source: {}}
	destinations := make(map[string]struct{})
	stack := append([]string(nil), adjacency[source]...)
	for len(stack) > 0 {
		last := len(stack) - 1
		node := stack[last]
		stack = stack[:last]
		if node == source {
			destinations[node] = struct{}{}
			continue
		}
		if _, exists := visited[node]; exists {
			continue
		}
		visited[node] = struct{}{}
		destinations[node] = struct{}{}
		stack = append(stack, adjacency[node]...)
	}
	return destinations
}

func cloneRecursiveReachabilityAncestors(ancestors map[string]map[string]struct{}) map[string]map[string]struct{} {
	clone := make(map[string]map[string]struct{}, len(ancestors))
	for destination, sources := range ancestors {
		clone[destination] = sources
	}
	return clone
}
