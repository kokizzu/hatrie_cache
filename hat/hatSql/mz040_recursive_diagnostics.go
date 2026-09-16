package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var (
	// ErrIncrementalRecursiveReachabilityLimitExceeded reports an opt-in
	// convergence budget that would be exceeded by a mutation batch.
	ErrIncrementalRecursiveReachabilityLimitExceeded = errors.New("hatSql: recursive reachability convergence limit exceeded")
	// ErrIncrementalRecursiveReachabilityOptionsInvalid reports a negative
	// convergence budget.
	ErrIncrementalRecursiveReachabilityOptionsInvalid = errors.New("hatSql: recursive reachability options are invalid")
)

// RecursiveReachabilityApplyOptions bounds the work of ApplyWithOptions. A
// zero value leaves a bound unlimited. The bounds are checked before the
// mutation is committed, so a rejected batch leaves the graph unchanged.
type RecursiveReachabilityApplyOptions struct {
	MaxAffectedSources int
	MaxIterations      int
	MaxTraversalSteps  int
	MaxEmittedUpdates  int
}

// RecursiveReachabilityApplyStats describes the convergence work observed for
// one ApplyWithOptions call. On a limit error, the fields contain the work
// observed before rejection.
type RecursiveReachabilityApplyStats struct {
	MutationCount        int
	CandidateEdges       int
	AffectedSources      int
	TraversalSteps       int
	MaxIterations        int
	ReachablePairsBefore int
	ReachablePairsAfter  int
	AddedPairs           int
	RemovedPairs         int
	EmittedUpdates       int
}

// RecursiveReachabilityLimitError identifies the budget and observed value
// that rejected a mutation batch. It wraps
// ErrIncrementalRecursiveReachabilityLimitExceeded.
type RecursiveReachabilityLimitError struct {
	Limit    string
	Maximum  int
	Observed int
}

func (err RecursiveReachabilityLimitError) Error() string {
	return fmt.Sprintf("%s: %s=%d exceeds maximum %d", ErrIncrementalRecursiveReachabilityLimitExceeded, err.Limit, err.Observed, err.Maximum)
}

func (err RecursiveReachabilityLimitError) Unwrap() error {
	return ErrIncrementalRecursiveReachabilityLimitExceeded
}

// ApplyWithOptions atomically applies mutations to a mutable recursive
// reachability maintainer while collecting convergence diagnostics. Unlike
// Apply, this opt-in path recomputes affected sources through a bounded
// convergence plan so callers can reject unexpectedly large recursive work.
// The existing Apply path remains unchanged and keeps its optimized fast
// paths. ApplyWithOptions is unavailable on append-only maintainers.
func (reachability *IncrementalRecursiveReachability) ApplyWithOptions(mutations []RecursiveReachabilityMutation, options RecursiveReachabilityApplyOptions) ([]DifferentialRow, RecursiveReachabilityApplyStats, error) {
	var stats RecursiveReachabilityApplyStats
	if reachability == nil {
		return nil, stats, ErrIncrementalRecursiveReachabilityNil
	}
	if err := options.validate(); err != nil {
		return nil, stats, err
	}
	if reachability.mutableEdges == nil {
		return nil, stats, ErrIncrementalRecursiveReachabilityMutationsDisabled
	}
	if len(mutations) == 0 {
		return nil, stats, nil
	}

	reachability.mu.Lock()
	defer reachability.mu.Unlock()

	stats.MutationCount = len(mutations)
	normalized, candidate, err := reachability.normalizeRecursiveReachabilityMutationsLocked(mutations)
	if err != nil {
		return nil, stats, err
	}
	stats.CandidateEdges = len(candidate)
	if len(normalized) == 1 && normalized[0].Kind == RecursiveReachabilityInsert && options.MaxIterations == 0 && options.MaxTraversalSteps == 0 {
		return reachability.applyRecursiveReachabilitySingleInsertWithOptionsLocked(normalized[0], options, stats)
	}
	updates, stats, err := reachability.applyRecursiveReachabilityWithOptionsLocked(normalized, candidate, options, stats)
	return updates, stats, err
}

func (options RecursiveReachabilityApplyOptions) validate() error {
	if options.MaxAffectedSources < 0 || options.MaxIterations < 0 || options.MaxTraversalSteps < 0 || options.MaxEmittedUpdates < 0 {
		return fmt.Errorf("%w: limits must be non-negative", ErrIncrementalRecursiveReachabilityOptionsInvalid)
	}
	return nil
}

func (reachability *IncrementalRecursiveReachability) normalizeRecursiveReachabilityMutationsLocked(mutations []RecursiveReachabilityMutation) ([]RecursiveReachabilityMutation, map[string]RecursiveReachabilityEdge, error) {
	normalized := make([]RecursiveReachabilityMutation, len(mutations))
	seen := make(map[string]struct{}, len(mutations))
	candidate := make(map[string]RecursiveReachabilityEdge, len(reachability.mutableEdges)+len(mutations))
	for key, edge := range reachability.mutableEdges {
		candidate[key] = edge
	}
	for index, mutation := range mutations {
		kind := RecursiveReachabilityMutationKind(toUpperTrimmed(string(mutation.Kind)))
		if kind != RecursiveReachabilityInsert && kind != RecursiveReachabilityUpdate && kind != RecursiveReachabilityDelete {
			return nil, nil, fmt.Errorf("recursive reachability mutation %d: %w", index, ErrIncrementalRecursiveReachabilityMutationInvalid)
		}
		key := mutation.Key
		if len(key) == 0 || toUpperTrimmed(key) == "" {
			return nil, nil, fmt.Errorf("recursive reachability mutation %d: %w", index, ErrIncrementalRecursiveReachabilityMutationKeyRequired)
		}
		if _, exists := seen[key]; exists {
			return nil, nil, fmt.Errorf("recursive reachability mutation %d key %q: %w", index, key, ErrIncrementalRecursiveReachabilityMutationDuplicate)
		}
		seen[key] = struct{}{}
		_, exists := reachability.mutableEdges[key]
		switch kind {
		case RecursiveReachabilityInsert:
			if exists {
				return nil, nil, fmt.Errorf("recursive reachability mutation %d key %q: %w", index, key, ErrIncrementalRecursiveReachabilityMutationDuplicate)
			}
			edge := RecursiveReachabilityEdge{Key: key, From: mutation.From, To: mutation.To}
			if err := validateRecursiveReachabilityEdge(edge); err != nil {
				return nil, nil, fmt.Errorf("recursive reachability mutation %d: %w", index, err)
			}
			candidate[key] = edge
		case RecursiveReachabilityUpdate:
			if !exists {
				return nil, nil, fmt.Errorf("recursive reachability mutation %d key %q: %w", index, key, ErrIncrementalRecursiveReachabilityMutationMissing)
			}
			edge := RecursiveReachabilityEdge{Key: key, From: mutation.From, To: mutation.To}
			if err := validateRecursiveReachabilityEdge(edge); err != nil {
				return nil, nil, fmt.Errorf("recursive reachability mutation %d: %w", index, err)
			}
			candidate[key] = edge
		case RecursiveReachabilityDelete:
			if !exists {
				return nil, nil, fmt.Errorf("recursive reachability mutation %d key %q: %w", index, key, ErrIncrementalRecursiveReachabilityMutationMissing)
			}
			delete(candidate, key)
		}
		normalized[index] = RecursiveReachabilityMutation{Kind: kind, Key: key, From: mutation.From, To: mutation.To}
	}
	return normalized, candidate, nil
}

func (reachability *IncrementalRecursiveReachability) applyRecursiveReachabilityWithOptionsLocked(mutations []RecursiveReachabilityMutation, candidate map[string]RecursiveReachabilityEdge, options RecursiveReachabilityApplyOptions, stats RecursiveReachabilityApplyStats) ([]DifferentialRow, RecursiveReachabilityApplyStats, error) {
	adjacency, reverse := buildRecursiveReachabilityGraph(candidate)
	affectedSources := make(map[string]struct{})
	budget := recursiveReachabilityConvergenceBudget{options: options, stats: &stats}
	for _, mutation := range mutations {
		if mutation.Kind == RecursiveReachabilityUpdate || mutation.Kind == RecursiveReachabilityDelete {
			oldEdge := reachability.mutableEdges[mutation.Key]
			affectedSources[oldEdge.From] = struct{}{}
			for source := range reachability.ancestors[oldEdge.From] {
				affectedSources[source] = struct{}{}
			}
			sources, err := recursiveReachabilityReverseSourcesWithBudget(oldEdge.From, reverse, &budget)
			if err != nil {
				return nil, stats, err
			}
			for source := range sources {
				affectedSources[source] = struct{}{}
			}
		}
		if mutation.Kind == RecursiveReachabilityInsert || mutation.Kind == RecursiveReachabilityUpdate {
			sources, err := recursiveReachabilityReverseSourcesWithBudget(mutation.From, reverse, &budget)
			if err != nil {
				return nil, stats, err
			}
			for source := range sources {
				affectedSources[source] = struct{}{}
			}
		}
	}
	stats.AffectedSources = len(affectedSources)
	if options.MaxAffectedSources > 0 && stats.AffectedSources > options.MaxAffectedSources {
		return nil, stats, RecursiveReachabilityLimitError{Limit: "affected_sources", Maximum: options.MaxAffectedSources, Observed: stats.AffectedSources}
	}

	stats.ReachablePairsBefore = recursiveReachabilityPairCount(reachability.reachable)
	nextReachable := make(map[string]map[string]struct{}, len(reachability.reachable)+len(affectedSources))
	for source, destinations := range reachability.reachable {
		nextReachable[source] = destinations
	}
	orderedSources := make([]string, 0, len(affectedSources))
	for source := range affectedSources {
		orderedSources = append(orderedSources, source)
	}
	sort.Strings(orderedSources)
	for _, source := range orderedSources {
		destinations, err := recursiveReachabilityFromWithBudget(source, adjacency, &budget)
		if err != nil {
			return nil, stats, err
		}
		if len(destinations) == 0 {
			delete(nextReachable, source)
			continue
		}
		nextReachable[source] = destinations
	}

	changes, addedPairs, removedPairs, err := collectRecursiveReachabilityChangesWithLimit(reachability.reachable, nextReachable, affectedSources, options.MaxEmittedUpdates)
	stats.AddedPairs = addedPairs
	stats.RemovedPairs = removedPairs
	stats.EmittedUpdates = addedPairs + removedPairs
	if err != nil {
		return nil, stats, err
	}
	stats.ReachablePairsAfter = recursiveReachabilityPairCount(nextReachable)

	nextAncestors := cloneRecursiveReachabilityAncestors(reachability.ancestors)
	clonedAncestors := make(map[string]struct{})
	updates := make([]DifferentialRow, 0, len(changes))
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
	return updates, stats, nil
}

func (reachability *IncrementalRecursiveReachability) applyRecursiveReachabilitySingleInsertWithOptionsLocked(mutation RecursiveReachabilityMutation, options RecursiveReachabilityApplyOptions, stats RecursiveReachabilityApplyStats) ([]DifferentialRow, RecursiveReachabilityApplyStats, error) {
	stats.ReachablePairsBefore = recursiveReachabilityPairCount(reachability.reachable)
	sources := recursiveReachabilityEndpoints(reachability.ancestors[mutation.From], mutation.From)
	stats.AffectedSources = len(sources)
	if options.MaxAffectedSources > 0 && stats.AffectedSources > options.MaxAffectedSources {
		return nil, stats, RecursiveReachabilityLimitError{Limit: "affected_sources", Maximum: options.MaxAffectedSources, Observed: stats.AffectedSources}
	}
	destinations := recursiveReachabilityEndpoints(reachability.reachable[mutation.To], mutation.To)
	for _, source := range sources {
		for _, destination := range destinations {
			if _, exists := reachability.reachable[source][destination]; exists {
				continue
			}
			stats.AddedPairs++
			stats.EmittedUpdates++
			if options.MaxEmittedUpdates > 0 && stats.EmittedUpdates > options.MaxEmittedUpdates {
				return nil, stats, RecursiveReachabilityLimitError{Limit: "emitted_updates", Maximum: options.MaxEmittedUpdates, Observed: stats.EmittedUpdates}
			}
		}
	}
	updates, err := reachability.appendLocked([]RecursiveReachabilityEdge{{Key: mutation.Key, From: mutation.From, To: mutation.To}})
	if err != nil {
		return nil, stats, err
	}
	if len(updates) != stats.EmittedUpdates {
		return nil, stats, fmt.Errorf("recursive reachability bounded insert produced %d updates, counted %d", len(updates), stats.EmittedUpdates)
	}
	stats.ReachablePairsAfter = stats.ReachablePairsBefore + stats.AddedPairs
	return updates, stats, nil
}

type recursiveReachabilityConvergenceBudget struct {
	options RecursiveReachabilityApplyOptions
	stats   *RecursiveReachabilityApplyStats
}

func (budget *recursiveReachabilityConvergenceBudget) visit(depth int) error {
	if depth > budget.stats.MaxIterations {
		budget.stats.MaxIterations = depth
	}
	if budget.options.MaxIterations > 0 && depth > budget.options.MaxIterations {
		return RecursiveReachabilityLimitError{Limit: "iterations", Maximum: budget.options.MaxIterations, Observed: depth}
	}
	budget.stats.TraversalSteps++
	if budget.options.MaxTraversalSteps > 0 && budget.stats.TraversalSteps > budget.options.MaxTraversalSteps {
		return RecursiveReachabilityLimitError{Limit: "traversal_steps", Maximum: budget.options.MaxTraversalSteps, Observed: budget.stats.TraversalSteps}
	}
	return nil
}

type recursiveReachabilityVisit struct {
	node  string
	depth int
}

func recursiveReachabilityReverseSourcesWithBudget(start string, reverse map[string][]string, budget *recursiveReachabilityConvergenceBudget) (map[string]struct{}, error) {
	sources := map[string]struct{}{start: {}}
	stack := []recursiveReachabilityVisit{{node: start}}
	for len(stack) > 0 {
		last := len(stack) - 1
		visit := stack[last]
		stack = stack[:last]
		if err := budget.visit(visit.depth); err != nil {
			return nil, err
		}
		for _, source := range reverse[visit.node] {
			if _, exists := sources[source]; exists {
				continue
			}
			sources[source] = struct{}{}
			stack = append(stack, recursiveReachabilityVisit{node: source, depth: visit.depth + 1})
		}
	}
	return sources, nil
}

func recursiveReachabilityFromWithBudget(source string, adjacency map[string][]string, budget *recursiveReachabilityConvergenceBudget) (map[string]struct{}, error) {
	if len(adjacency[source]) == 0 {
		return nil, nil
	}
	visited := map[string]struct{}{source: {}}
	destinations := make(map[string]struct{})
	stack := make([]recursiveReachabilityVisit, 0, len(adjacency[source]))
	for _, node := range adjacency[source] {
		stack = append(stack, recursiveReachabilityVisit{node: node, depth: 1})
	}
	for len(stack) > 0 {
		last := len(stack) - 1
		visit := stack[last]
		stack = stack[:last]
		if visit.node == source {
			if err := budget.visit(visit.depth); err != nil {
				return nil, err
			}
			destinations[visit.node] = struct{}{}
			continue
		}
		if _, exists := visited[visit.node]; exists {
			continue
		}
		if err := budget.visit(visit.depth); err != nil {
			return nil, err
		}
		visited[visit.node] = struct{}{}
		destinations[visit.node] = struct{}{}
		for _, node := range adjacency[visit.node] {
			stack = append(stack, recursiveReachabilityVisit{node: node, depth: visit.depth + 1})
		}
	}
	return destinations, nil
}

func recursiveReachabilityPairCount(reachable map[string]map[string]struct{}) int {
	count := 0
	for _, destinations := range reachable {
		count += len(destinations)
	}
	return count
}

func collectRecursiveReachabilityChangesWithLimit(oldReachable, newReachable map[string]map[string]struct{}, affectedSources map[string]struct{}, maxUpdates int) ([]recursiveReachabilityChange, int, int, error) {
	changes := make([]recursiveReachabilityChange, 0)
	addedPairs := 0
	removedPairs := 0
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
			if wasPresent == isPresent {
				continue
			}
			if isPresent {
				addedPairs++
			} else {
				removedPairs++
			}
			observed := addedPairs + removedPairs
			if maxUpdates > 0 && observed > maxUpdates {
				return nil, addedPairs, removedPairs, RecursiveReachabilityLimitError{Limit: "emitted_updates", Maximum: maxUpdates, Observed: observed}
			}
			changes = append(changes, recursiveReachabilityChange{from: source, to: destination, present: isPresent})
		}
	}
	sort.Slice(changes, func(left, right int) bool {
		leftKey := recursiveReachabilityPairKey(changes[left].from, changes[left].to)
		rightKey := recursiveReachabilityPairKey(changes[right].from, changes[right].to)
		return leftKey < rightKey
	})
	return changes, addedPairs, removedPairs, nil
}

func toUpperTrimmed(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}
