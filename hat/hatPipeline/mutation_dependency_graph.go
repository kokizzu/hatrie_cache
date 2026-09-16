package hatPipeline

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MutationState describes the lifecycle state of a mutation.
type MutationState uint8

const (
	// MutationPending is waiting for one or more dependencies.
	MutationPending MutationState = iota
	// MutationReady can be started by the caller.
	MutationReady
	// MutationRunning is currently owned by a caller.
	MutationRunning
	// MutationCompleted finished successfully.
	MutationCompleted
	// MutationFailed finished with an error and can be retried.
	MutationFailed
	// MutationBlocked cannot run because a dependency failed.
	MutationBlocked
)

// String returns the stable, human-readable state name.
func (s MutationState) String() string {
	switch s {
	case MutationPending:
		return "pending"
	case MutationReady:
		return "ready"
	case MutationRunning:
		return "running"
	case MutationCompleted:
		return "completed"
	case MutationFailed:
		return "failed"
	case MutationBlocked:
		return "blocked"
	default:
		return "unknown"
	}
}

var (
	// ErrMutationDependencyGraphNil is returned by a method on a nil graph.
	ErrMutationDependencyGraphNil = errors.New("mutation dependency graph is nil")
	// ErrMutationAlreadyExists means that a mutation ID is already registered.
	ErrMutationAlreadyExists = errors.New("mutation already exists")
	// ErrMutationUnknown means that a mutation ID is not registered.
	ErrMutationUnknown = errors.New("unknown mutation")
	// ErrMutationDependencyUnknown means that a dependency is not registered.
	ErrMutationDependencyUnknown = errors.New("unknown mutation dependency")
	// ErrMutationDependencyDuplicate means that a mutation lists a dependency twice.
	ErrMutationDependencyDuplicate = errors.New("duplicate mutation dependency")
	// ErrMutationDependencyCycle means that a snapshot contains a dependency cycle.
	ErrMutationDependencyCycle = errors.New("mutation dependency cycle")
	// ErrMutationCycle is kept as a concise alias for callers that use the shorter name.
	ErrMutationCycle = ErrMutationDependencyCycle
	// ErrMutationNotReady means that a mutation cannot be started yet.
	ErrMutationNotReady = errors.New("mutation is not ready")
	// ErrMutationNotRunning means that a mutation does not have an active lease.
	ErrMutationNotRunning = errors.New("mutation is not running")
	// ErrMutationNotFailed means that a mutation has no failure to retry.
	ErrMutationNotFailed = errors.New("mutation is not failed")
	// ErrMutationSnapshotInvalid means that a snapshot is malformed.
	ErrMutationSnapshotInvalid = errors.New("invalid mutation dependency graph snapshot")
)

const maxMutationDependencyGraphNodes = 1_000_000

type mutationNode struct {
	id           string
	dependencies []string
	dependents   []string
	remaining    int
	state        MutationState
	err          string
}

// MutationStatus is a point-in-time view of one mutation.
type MutationStatus struct {
	ID           string
	Dependencies []string
	Remaining    int
	State        MutationState
	Error        string
}

// MutationDependencyGraphProgress summarizes all mutations in a graph.
type MutationDependencyGraphProgress struct {
	Total     int
	Ready     int
	Pending   int
	Running   int
	Completed int
	Failed    int
	Blocked   int
}

// MutationNodeSnapshot is the portable representation of one graph node.
// Running, ready, and blocked nodes are re-queued or recomputed on restore.
type MutationNodeSnapshot struct {
	ID           string
	Dependencies []string
	State        MutationState
	Error        string
}

// MutationDependencyGraphSnapshot is an explicit checkpoint of a dependency graph.
// The graph does not write this value to disk automatically; callers choose the
// storage and checkpoint cadence that fit their durability requirements.
type MutationDependencyGraphSnapshot struct {
	Nodes []MutationNodeSnapshot
}

// MutationDependencyGraph tracks caller-owned work with explicit dependencies.
//
// Add requires dependencies to have already been added. Start, Complete, and
// Retry only change graph metadata; the caller remains responsible for running
// the actual mutation and persisting Snapshot values when needed. Methods are
// safe for concurrent callers.
type MutationDependencyGraph struct {
	mu    sync.RWMutex
	nodes map[string]*mutationNode
	ready map[string]struct{}
}

// NewMutationDependencyGraph creates an empty dependency graph.
func NewMutationDependencyGraph() *MutationDependencyGraph {
	return &MutationDependencyGraph{
		nodes: make(map[string]*mutationNode),
		ready: make(map[string]struct{}),
	}
}

func (g *MutationDependencyGraph) ensureMapsLocked() {
	if g.nodes == nil {
		g.nodes = make(map[string]*mutationNode)
	}
	if g.ready == nil {
		g.ready = make(map[string]struct{})
	}
}

func normalizeMutationID(id string) string {
	return strings.TrimSpace(id)
}

func mutationIDError(kind error, id string) error {
	return fmt.Errorf("%w: %q", kind, id)
}

func (g *MutationDependencyGraph) nodeLocked(id string) (*mutationNode, error) {
	if g == nil {
		return nil, ErrMutationDependencyGraphNil
	}
	node, ok := g.nodes[id]
	if !ok {
		return nil, mutationIDError(ErrMutationUnknown, id)
	}
	return node, nil
}

// Add registers id and its dependencies. Dependencies must already exist.
// A newly unblocked node is immediately visible in Ready.
func (g *MutationDependencyGraph) Add(id string, dependencies ...string) error {
	if g == nil {
		return ErrMutationDependencyGraphNil
	}
	id = normalizeMutationID(id)
	if id == "" {
		return mutationIDError(ErrMutationSnapshotInvalid, id)
	}

	deps := make([]string, 0, len(dependencies))
	seen := make(map[string]struct{}, len(dependencies))
	for _, dependency := range dependencies {
		dependency = normalizeMutationID(dependency)
		if dependency == "" {
			return mutationIDError(ErrMutationDependencyUnknown, dependency)
		}
		if dependency == id {
			return mutationIDError(ErrMutationDependencyCycle, id)
		}
		if _, ok := seen[dependency]; ok {
			return mutationIDError(ErrMutationDependencyDuplicate, dependency)
		}
		seen[dependency] = struct{}{}
		deps = append(deps, dependency)
	}
	sort.Strings(deps)

	g.mu.Lock()
	defer g.mu.Unlock()
	g.ensureMapsLocked()
	if _, ok := g.nodes[id]; ok {
		return mutationIDError(ErrMutationAlreadyExists, id)
	}
	for _, dependency := range deps {
		if _, ok := g.nodes[dependency]; !ok {
			return mutationIDError(ErrMutationDependencyUnknown, dependency)
		}
	}

	node := &mutationNode{
		id:           id,
		dependencies: deps,
		state:        MutationPending,
	}
	for _, dependency := range deps {
		parent := g.nodes[dependency]
		parent.dependents = append(parent.dependents, id)
		if parent.state != MutationCompleted {
			node.remaining++
		}
	}
	g.nodes[id] = node
	if node.remaining == 0 {
		g.ready[id] = struct{}{}
	}
	return nil
}

// Ready returns a sorted copy of mutations that can be started.
func (g *MutationDependencyGraph) Ready() []string {
	return g.ReadyInto(nil, 0)
}

// ReadyInto appends a sorted ready list to dst and returns the resulting slice.
// A positive limit bounds the number of returned IDs. The caller may reuse dst
// between polling cycles to avoid a result allocation.
func (g *MutationDependencyGraph) ReadyInto(dst []string, limit int) []string {
	dst = dst[:0]
	if g == nil {
		return dst
	}
	g.mu.RLock()
	if len(g.ready) == 0 {
		g.mu.RUnlock()
		return dst
	}
	for id := range g.ready {
		dst = append(dst, id)
	}
	g.mu.RUnlock()
	sort.Strings(dst)
	if limit > 0 && len(dst) > limit {
		dst = dst[:limit]
	}
	return dst
}

// Start claims a ready mutation for the caller.
func (g *MutationDependencyGraph) Start(id string) error {
	if g == nil {
		return ErrMutationDependencyGraphNil
	}
	id = normalizeMutationID(id)
	g.mu.Lock()
	defer g.mu.Unlock()
	node, err := g.nodeLocked(id)
	if err != nil {
		return err
	}
	if node.state != MutationPending || node.remaining != 0 {
		return mutationIDError(ErrMutationNotReady, id)
	}
	delete(g.ready, id)
	node.state = MutationRunning
	return nil
}

// Complete records success or failure for a running mutation. A non-nil err
// leaves the node retryable and keeps dependent work blocked until it succeeds.
func (g *MutationDependencyGraph) Complete(id string, runErr error) error {
	if g == nil {
		return ErrMutationDependencyGraphNil
	}
	id = normalizeMutationID(id)
	g.mu.Lock()
	defer g.mu.Unlock()
	node, err := g.nodeLocked(id)
	if err != nil {
		return err
	}
	if node.state != MutationRunning {
		return mutationIDError(ErrMutationNotRunning, id)
	}
	if runErr != nil {
		node.state = MutationFailed
		node.err = runErr.Error()
		return nil
	}

	node.state = MutationCompleted
	node.err = ""
	for _, dependentID := range node.dependents {
		dependent := g.nodes[dependentID]
		if dependent.remaining > 0 {
			dependent.remaining--
		}
		if dependent.remaining == 0 && dependent.state == MutationPending {
			g.ready[dependentID] = struct{}{}
		}
	}
	return nil
}

// Retry moves a failed mutation back to the ready state once its dependencies
// have completed successfully.
func (g *MutationDependencyGraph) Retry(id string) error {
	if g == nil {
		return ErrMutationDependencyGraphNil
	}
	id = normalizeMutationID(id)
	g.mu.Lock()
	defer g.mu.Unlock()
	node, err := g.nodeLocked(id)
	if err != nil {
		return err
	}
	if node.state != MutationFailed {
		return mutationIDError(ErrMutationNotFailed, id)
	}
	if node.remaining != 0 || hasFailedDependencyLocked(g, node) {
		return mutationIDError(ErrMutationNotReady, id)
	}
	node.state = MutationPending
	node.err = ""
	g.ready[id] = struct{}{}
	return nil
}

func hasFailedDependencyLocked(g *MutationDependencyGraph, node *mutationNode) bool {
	for _, dependencyID := range node.dependencies {
		dependency := g.nodes[dependencyID]
		if dependency.state == MutationFailed || dependencyStatusLocked(g, dependency, make(map[string]struct{})) == MutationBlocked {
			return true
		}
	}
	return false
}

func dependencyStatusLocked(g *MutationDependencyGraph, node *mutationNode, visiting map[string]struct{}) MutationState {
	if node == nil {
		return MutationBlocked
	}
	switch node.state {
	case MutationCompleted, MutationRunning, MutationFailed:
		return node.state
	}
	if _, ok := visiting[node.id]; ok {
		return MutationBlocked
	}
	visiting[node.id] = struct{}{}
	defer delete(visiting, node.id)
	for _, dependencyID := range node.dependencies {
		dependency := g.nodes[dependencyID]
		state := dependencyStatusLocked(g, dependency, visiting)
		if state == MutationFailed || state == MutationBlocked {
			return MutationBlocked
		}
	}
	if node.remaining == 0 {
		return MutationReady
	}
	return MutationPending
}

func (g *MutationDependencyGraph) statusLocked(node *mutationNode) MutationStatus {
	status := MutationStatus{
		ID:           node.id,
		Dependencies: append([]string(nil), node.dependencies...),
		Remaining:    node.remaining,
		State:        dependencyStatusLocked(g, node, make(map[string]struct{})),
		Error:        node.err,
	}
	return status
}

// Status returns a copy of the current status for id.
func (g *MutationDependencyGraph) Status(id string) (MutationStatus, bool) {
	if g == nil {
		return MutationStatus{}, false
	}
	id = normalizeMutationID(id)
	g.mu.RLock()
	defer g.mu.RUnlock()
	node, ok := g.nodes[id]
	if !ok {
		return MutationStatus{}, false
	}
	return g.statusLocked(node), true
}

// Statuses returns all mutation statuses in ID order.
func (g *MutationDependencyGraph) Statuses() []MutationStatus {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := make([]string, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	statuses := make([]MutationStatus, 0, len(ids))
	for _, id := range ids {
		statuses = append(statuses, g.statusLocked(g.nodes[id]))
	}
	return statuses
}

// Progress returns lifecycle counts for the graph.
func (g *MutationDependencyGraph) Progress() MutationDependencyGraphProgress {
	var progress MutationDependencyGraphProgress
	if g == nil {
		return progress
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	progress.Total = len(g.nodes)
	for _, node := range g.nodes {
		switch g.statusLocked(node).State {
		case MutationReady:
			progress.Ready++
		case MutationPending:
			progress.Pending++
		case MutationRunning:
			progress.Running++
		case MutationCompleted:
			progress.Completed++
		case MutationFailed:
			progress.Failed++
		case MutationBlocked:
			progress.Blocked++
		}
	}
	return progress
}

// Snapshot returns a deterministic copy of the graph metadata.
func (g *MutationDependencyGraph) Snapshot() MutationDependencyGraphSnapshot {
	if g == nil {
		return MutationDependencyGraphSnapshot{}
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := make([]string, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	snapshot := MutationDependencyGraphSnapshot{
		Nodes: make([]MutationNodeSnapshot, 0, len(ids)),
	}
	for _, id := range ids {
		node := g.nodes[id]
		status := g.statusLocked(node)
		snapshot.Nodes = append(snapshot.Nodes, MutationNodeSnapshot{
			ID:           id,
			Dependencies: append([]string(nil), status.Dependencies...),
			State:        status.State,
			Error:        status.Error,
		})
	}
	return snapshot
}

func validMutationState(state MutationState) bool {
	return state <= MutationBlocked
}

// RestoreMutationDependencyGraph validates and restores a graph checkpoint.
// Running work is returned to pending so an interrupted caller can resume it.
func RestoreMutationDependencyGraph(snapshot MutationDependencyGraphSnapshot) (*MutationDependencyGraph, error) {
	if len(snapshot.Nodes) > maxMutationDependencyGraphNodes {
		return nil, fmt.Errorf("%w: too many nodes", ErrMutationSnapshotInvalid)
	}
	g := NewMutationDependencyGraph()
	for _, saved := range snapshot.Nodes {
		id := normalizeMutationID(saved.ID)
		if id == "" {
			return nil, fmt.Errorf("%w: empty mutation ID", ErrMutationSnapshotInvalid)
		}
		if _, ok := g.nodes[id]; ok {
			return nil, mutationIDError(ErrMutationAlreadyExists, id)
		}
		if !validMutationState(saved.State) {
			return nil, fmt.Errorf("%w: mutation %q has state %d", ErrMutationSnapshotInvalid, id, saved.State)
		}
		deps := make([]string, 0, len(saved.Dependencies))
		seen := make(map[string]struct{}, len(saved.Dependencies))
		for _, dependency := range saved.Dependencies {
			dependency = normalizeMutationID(dependency)
			if dependency == "" {
				return nil, fmt.Errorf("%w: mutation %q has an empty dependency", ErrMutationSnapshotInvalid, id)
			}
			if dependency == id {
				return nil, mutationIDError(ErrMutationDependencyCycle, id)
			}
			if _, ok := seen[dependency]; ok {
				return nil, mutationIDError(ErrMutationDependencyDuplicate, dependency)
			}
			seen[dependency] = struct{}{}
			deps = append(deps, dependency)
		}
		sort.Strings(deps)
		state := saved.State
		if state == MutationReady || state == MutationRunning || state == MutationBlocked {
			state = MutationPending
		}
		errorText := saved.Error
		if state != MutationFailed {
			errorText = ""
		}
		g.nodes[id] = &mutationNode{
			id:           id,
			dependencies: deps,
			state:        state,
			err:          errorText,
		}
	}

	for _, node := range g.nodes {
		for _, dependencyID := range node.dependencies {
			dependency, ok := g.nodes[dependencyID]
			if !ok {
				return nil, mutationIDError(ErrMutationDependencyUnknown, dependencyID)
			}
			dependency.dependents = append(dependency.dependents, node.id)
		}
	}
	if hasMutationDependencyCycle(g.nodes) {
		return nil, ErrMutationDependencyCycle
	}
	for _, node := range g.nodes {
		for _, dependencyID := range node.dependencies {
			if g.nodes[dependencyID].state != MutationCompleted {
				node.remaining++
			}
		}
		if node.state == MutationCompleted && node.remaining != 0 {
			return nil, fmt.Errorf("%w: completed mutation %q has incomplete dependencies", ErrMutationSnapshotInvalid, node.id)
		}
		if node.state == MutationPending && node.remaining == 0 {
			g.ready[node.id] = struct{}{}
		}
	}
	return g, nil
}

func hasMutationDependencyCycle(nodes map[string]*mutationNode) bool {
	const (
		unseen = uint8(iota)
		visiting
		done
	)
	colors := make(map[string]uint8, len(nodes))
	var visit func(string) bool
	visit = func(id string) bool {
		switch colors[id] {
		case visiting:
			return true
		case done:
			return false
		}
		colors[id] = visiting
		for _, dependencyID := range nodes[id].dependencies {
			if visit(dependencyID) {
				return true
			}
		}
		colors[id] = done
		return false
	}
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if visit(id) {
			return true
		}
	}
	return false
}
