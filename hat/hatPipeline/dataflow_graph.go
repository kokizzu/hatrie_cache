package hatPipeline

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultDataflowGraphMaxNodes is used when DataflowGraphOptions.MaxNodes is zero.
	DefaultDataflowGraphMaxNodes = 4096
	// DefaultDataflowGraphMaxEdges is used when DataflowGraphOptions.MaxEdges is zero.
	DefaultDataflowGraphMaxEdges = 16384

	maxDataflowGraphNodes   = 1 << 20
	maxDataflowGraphEdges   = 1 << 22
	maxDataflowNodeIDSize   = 256
	maxDataflowNodeKindSize = 128
)

var (
	// ErrDataflowGraphNil reports a method call on a nil graph.
	ErrDataflowGraphNil = errors.New("hatPipeline: dataflow graph is nil")
	// ErrDataflowGraphInvalid reports invalid graph or node input.
	ErrDataflowGraphInvalid = errors.New("hatPipeline: invalid dataflow graph input")
	// ErrDataflowGraphDuplicateNode reports a repeated node ID.
	ErrDataflowGraphDuplicateNode = errors.New("hatPipeline: dataflow graph node already exists")
	// ErrDataflowGraphDuplicateEdge reports a repeated edge.
	ErrDataflowGraphDuplicateEdge = errors.New("hatPipeline: dataflow graph edge already exists")
	// ErrDataflowGraphNodeNotFound reports an unknown node ID.
	ErrDataflowGraphNodeNotFound = errors.New("hatPipeline: dataflow graph node not found")
	// ErrDataflowGraphEdgeNotFound reports an unknown edge.
	ErrDataflowGraphEdgeNotFound = errors.New("hatPipeline: dataflow graph edge not found")
	// ErrDataflowGraphCapacity reports a configured node or edge limit.
	ErrDataflowGraphCapacity = errors.New("hatPipeline: dataflow graph capacity exceeded")
	// ErrDataflowGraphCycle reports an edge that would create a cycle.
	ErrDataflowGraphCycle = errors.New("hatPipeline: dataflow graph contains a cycle")
)

// DataflowGraphOptions bounds the memory used by a graph. Zero values select
// the documented defaults; limits are deliberately finite to make graph
// introspection safe when IDs come from external metadata.
type DataflowGraphOptions struct {
	MaxNodes int
	MaxEdges int
}

// DataflowNode identifies one source, operator, sink, or other dataflow node.
// Kind is optional and defaults to "operator".
type DataflowNode struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

// DataflowEdge points from an upstream node to a downstream node.
type DataflowEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// DataflowGraphSnapshot is a deterministic, detached view of a graph.
// Nodes are sorted by ID and edges by From then To.
type DataflowGraphSnapshot struct {
	Nodes []DataflowNode `json:"nodes"`
	Edges []DataflowEdge `json:"edges"`
}

// DataflowGraph stores a bounded directed acyclic graph for operator
// introspection. Mutations and queries are safe to call concurrently.
type DataflowGraph struct {
	mu sync.RWMutex

	maxNodes int
	maxEdges int
	edges    int
	nodes    map[string]DataflowNode
	outgoing map[string]map[string]struct{}
	incoming map[string]map[string]struct{}
}

// NewDataflowGraph creates an empty bounded graph.
func NewDataflowGraph(options DataflowGraphOptions) (*DataflowGraph, error) {
	maxNodes, err := normalizeDataflowGraphLimit(options.MaxNodes, DefaultDataflowGraphMaxNodes, maxDataflowGraphNodes)
	if err != nil {
		return nil, fmt.Errorf("%w: max nodes", ErrDataflowGraphInvalid)
	}
	maxEdges, err := normalizeDataflowGraphLimit(options.MaxEdges, DefaultDataflowGraphMaxEdges, maxDataflowGraphEdges)
	if err != nil {
		return nil, fmt.Errorf("%w: max edges", ErrDataflowGraphInvalid)
	}
	return &DataflowGraph{
		maxNodes: maxNodes,
		maxEdges: maxEdges,
		nodes:    make(map[string]DataflowNode),
		outgoing: make(map[string]map[string]struct{}),
		incoming: make(map[string]map[string]struct{}),
	}, nil
}

// AddNode registers a node. IDs are trimmed and must be unique.
func (graph *DataflowGraph) AddNode(node DataflowNode) error {
	if graph == nil {
		return ErrDataflowGraphNil
	}
	id, kind, err := normalizeDataflowNode(node)
	if err != nil {
		return err
	}

	graph.mu.Lock()
	defer graph.mu.Unlock()
	graph.ensureInitializedLocked()
	if _, exists := graph.nodes[id]; exists {
		return fmt.Errorf("%w: %q", ErrDataflowGraphDuplicateNode, id)
	}
	if len(graph.nodes) >= graph.maxNodes {
		return fmt.Errorf("%w: maximum nodes is %d", ErrDataflowGraphCapacity, graph.maxNodes)
	}
	graph.nodes[id] = DataflowNode{ID: id, Kind: kind}
	graph.outgoing[id] = make(map[string]struct{})
	graph.incoming[id] = make(map[string]struct{})
	return nil
}

// AddEdge registers an upstream-to-downstream edge and rejects cycles.
func (graph *DataflowGraph) AddEdge(edge DataflowEdge) error {
	if graph == nil {
		return ErrDataflowGraphNil
	}
	from, to, err := normalizeDataflowEdge(edge)
	if err != nil {
		return err
	}

	graph.mu.Lock()
	defer graph.mu.Unlock()
	graph.ensureInitializedLocked()
	if _, exists := graph.nodes[from]; !exists {
		return fmt.Errorf("%w: %q", ErrDataflowGraphNodeNotFound, from)
	}
	if _, exists := graph.nodes[to]; !exists {
		return fmt.Errorf("%w: %q", ErrDataflowGraphNodeNotFound, to)
	}
	if _, exists := graph.outgoing[from][to]; exists {
		return fmt.Errorf("%w: %q -> %q", ErrDataflowGraphDuplicateEdge, from, to)
	}
	if from == to || graph.reachesLocked(to, from) {
		return fmt.Errorf("%w: %q -> %q", ErrDataflowGraphCycle, from, to)
	}
	if graph.edges >= graph.maxEdges {
		return fmt.Errorf("%w: maximum edges is %d", ErrDataflowGraphCapacity, graph.maxEdges)
	}
	graph.outgoing[from][to] = struct{}{}
	graph.incoming[to][from] = struct{}{}
	graph.edges++
	return nil
}

// RemoveEdge removes an existing edge.
func (graph *DataflowGraph) RemoveEdge(edge DataflowEdge) error {
	if graph == nil {
		return ErrDataflowGraphNil
	}
	from, to, err := normalizeDataflowEdge(edge)
	if err != nil {
		return err
	}

	graph.mu.Lock()
	defer graph.mu.Unlock()
	graph.ensureInitializedLocked()
	if _, exists := graph.nodes[from]; !exists {
		return fmt.Errorf("%w: %q", ErrDataflowGraphNodeNotFound, from)
	}
	if _, exists := graph.nodes[to]; !exists {
		return fmt.Errorf("%w: %q", ErrDataflowGraphNodeNotFound, to)
	}
	if _, exists := graph.outgoing[from][to]; !exists {
		return fmt.Errorf("%w: %q -> %q", ErrDataflowGraphEdgeNotFound, from, to)
	}
	delete(graph.outgoing[from], to)
	delete(graph.incoming[to], from)
	graph.edges--
	return nil
}

// RemoveNode removes a node and all of its incident edges.
func (graph *DataflowGraph) RemoveNode(id string) error {
	if graph == nil {
		return ErrDataflowGraphNil
	}
	id = strings.TrimSpace(id)
	if id == "" || len(id) > maxDataflowNodeIDSize {
		return fmt.Errorf("%w: node ID", ErrDataflowGraphInvalid)
	}

	graph.mu.Lock()
	defer graph.mu.Unlock()
	graph.ensureInitializedLocked()
	if _, exists := graph.nodes[id]; !exists {
		return fmt.Errorf("%w: %q", ErrDataflowGraphNodeNotFound, id)
	}
	for dependency := range graph.incoming[id] {
		delete(graph.outgoing[dependency], id)
		graph.edges--
	}
	for dependent := range graph.outgoing[id] {
		delete(graph.incoming[dependent], id)
		graph.edges--
	}
	delete(graph.incoming, id)
	delete(graph.outgoing, id)
	delete(graph.nodes, id)
	return nil
}

// Node returns a detached node description.
func (graph *DataflowGraph) Node(id string) (DataflowNode, bool) {
	if graph == nil {
		return DataflowNode{}, false
	}
	id = strings.TrimSpace(id)
	graph.mu.RLock()
	node, ok := graph.nodes[id]
	graph.mu.RUnlock()
	return node, ok
}

// Dependencies returns direct upstream node IDs in sorted order.
func (graph *DataflowGraph) Dependencies(id string) ([]string, error) {
	return graph.neighbors(id, true)
}

// Dependents returns direct downstream node IDs in sorted order.
func (graph *DataflowGraph) Dependents(id string) ([]string, error) {
	return graph.neighbors(id, false)
}

// Impact returns every transitive downstream node in sorted order. It is
// useful for estimating which operators must be rebuilt or invalidated after
// a source changes.
func (graph *DataflowGraph) Impact(id string) ([]string, error) {
	if graph == nil {
		return nil, ErrDataflowGraphNil
	}
	id = strings.TrimSpace(id)
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	if _, exists := graph.nodes[id]; !exists {
		return nil, fmt.Errorf("%w: %q", ErrDataflowGraphNodeNotFound, id)
	}

	visited := make(map[string]struct{}, len(graph.nodes))
	visited[id] = struct{}{}
	queue := make([]string, 0, len(graph.outgoing[id]))
	for dependent := range graph.outgoing[id] {
		queue = append(queue, dependent)
	}
	result := make([]string, 0, len(queue))
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if _, seen := visited[current]; seen {
			continue
		}
		visited[current] = struct{}{}
		result = append(result, current)
		for dependent := range graph.outgoing[current] {
			if _, seen := visited[dependent]; !seen {
				queue = append(queue, dependent)
			}
		}
	}
	sort.Strings(result)
	return result, nil
}

// TopologicalOrder returns all node IDs in deterministic dependency order.
func (graph *DataflowGraph) TopologicalOrder() ([]string, error) {
	if graph == nil {
		return nil, ErrDataflowGraphNil
	}
	graph.mu.RLock()
	defer graph.mu.RUnlock()

	indegree := make(map[string]int, len(graph.nodes))
	ready := make([]string, 0, len(graph.nodes))
	for id := range graph.nodes {
		degree := len(graph.incoming[id])
		indegree[id] = degree
		if degree == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	order := make([]string, 0, len(graph.nodes))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		for dependent := range graph.outgoing[id] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				insertSortedDataflowID(&ready, dependent)
			}
		}
	}
	if len(order) != len(graph.nodes) {
		return nil, ErrDataflowGraphCycle
	}
	return order, nil
}

// Snapshot returns a deterministic detached graph view.
func (graph *DataflowGraph) Snapshot() DataflowGraphSnapshot {
	if graph == nil {
		return DataflowGraphSnapshot{}
	}
	graph.mu.RLock()
	defer graph.mu.RUnlock()

	ids := make([]string, 0, len(graph.nodes))
	for id := range graph.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	snapshot := DataflowGraphSnapshot{
		Nodes: make([]DataflowNode, 0, len(ids)),
		Edges: make([]DataflowEdge, 0, graph.edges),
	}
	for _, id := range ids {
		snapshot.Nodes = append(snapshot.Nodes, graph.nodes[id])
		dependents := make([]string, 0, len(graph.outgoing[id]))
		for dependent := range graph.outgoing[id] {
			dependents = append(dependents, dependent)
		}
		sort.Strings(dependents)
		for _, dependent := range dependents {
			snapshot.Edges = append(snapshot.Edges, DataflowEdge{From: id, To: dependent})
		}
	}
	return snapshot
}

// Len returns the number of nodes in the graph.
func (graph *DataflowGraph) Len() int {
	if graph == nil {
		return 0
	}
	graph.mu.RLock()
	length := len(graph.nodes)
	graph.mu.RUnlock()
	return length
}

// EdgeCount returns the number of edges in the graph.
func (graph *DataflowGraph) EdgeCount() int {
	if graph == nil {
		return 0
	}
	graph.mu.RLock()
	edges := graph.edges
	graph.mu.RUnlock()
	return edges
}

func (graph *DataflowGraph) neighbors(id string, incoming bool) ([]string, error) {
	if graph == nil {
		return nil, ErrDataflowGraphNil
	}
	id = strings.TrimSpace(id)
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	if _, exists := graph.nodes[id]; !exists {
		return nil, fmt.Errorf("%w: %q", ErrDataflowGraphNodeNotFound, id)
	}
	adjacency := graph.outgoing[id]
	if incoming {
		adjacency = graph.incoming[id]
	}
	result := make([]string, 0, len(adjacency))
	for neighbor := range adjacency {
		result = append(result, neighbor)
	}
	sort.Strings(result)
	return result, nil
}

func (graph *DataflowGraph) ensureInitializedLocked() {
	if graph.maxNodes == 0 {
		graph.maxNodes = DefaultDataflowGraphMaxNodes
	}
	if graph.maxEdges == 0 {
		graph.maxEdges = DefaultDataflowGraphMaxEdges
	}
	if graph.nodes == nil {
		graph.nodes = make(map[string]DataflowNode)
	}
	if graph.outgoing == nil {
		graph.outgoing = make(map[string]map[string]struct{})
	}
	if graph.incoming == nil {
		graph.incoming = make(map[string]map[string]struct{})
	}
}

func (graph *DataflowGraph) reachesLocked(start, target string) bool {
	if start == target {
		return true
	}
	stack := []string{start}
	visited := make(map[string]struct{}, 8)
	for len(stack) > 0 {
		last := len(stack) - 1
		id := stack[last]
		stack = stack[:last]
		if id == target {
			return true
		}
		if _, seen := visited[id]; seen {
			continue
		}
		visited[id] = struct{}{}
		for dependent := range graph.outgoing[id] {
			if _, seen := visited[dependent]; !seen {
				stack = append(stack, dependent)
			}
		}
	}
	return false
}

func normalizeDataflowGraphLimit(value, defaultValue, maximum int) (int, error) {
	if value < 0 || value > maximum {
		return 0, ErrDataflowGraphInvalid
	}
	if value == 0 {
		return defaultValue, nil
	}
	return value, nil
}

func normalizeDataflowNode(node DataflowNode) (string, string, error) {
	id := strings.TrimSpace(node.ID)
	if id == "" || len(id) > maxDataflowNodeIDSize {
		return "", "", fmt.Errorf("%w: node ID must contain 1 to %d bytes", ErrDataflowGraphInvalid, maxDataflowNodeIDSize)
	}
	kind := strings.TrimSpace(node.Kind)
	if kind == "" {
		kind = "operator"
	}
	if len(kind) > maxDataflowNodeKindSize {
		return "", "", fmt.Errorf("%w: node kind exceeds %d bytes", ErrDataflowGraphInvalid, maxDataflowNodeKindSize)
	}
	return id, kind, nil
}

func normalizeDataflowEdge(edge DataflowEdge) (string, string, error) {
	from := strings.TrimSpace(edge.From)
	to := strings.TrimSpace(edge.To)
	if from == "" || len(from) > maxDataflowNodeIDSize || to == "" || len(to) > maxDataflowNodeIDSize {
		return "", "", fmt.Errorf("%w: edge node IDs must contain 1 to %d bytes", ErrDataflowGraphInvalid, maxDataflowNodeIDSize)
	}
	return from, to, nil
}

func insertSortedDataflowID(ids *[]string, id string) {
	index := sort.SearchStrings(*ids, id)
	*ids = append(*ids, "")
	copy((*ids)[index+1:], (*ids)[index:])
	(*ids)[index] = id
}
