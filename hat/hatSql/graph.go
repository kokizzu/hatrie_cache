package hatSql

import (
	"fmt"
	"sort"
	"sync"
)

// GraphEdge connects two application keys in a directed relationship graph.
type GraphEdge struct {
	From string
	To   string
}

// GraphTraversal records a node visited by a breadth-first traversal.
type GraphTraversal struct {
	Node  string
	Depth int
}

// KeyGraph stores directed relationships between stable application keys.
// Traversal methods are bounded explicitly to prevent unbounded graph expansion.
type KeyGraph struct {
	mu       sync.RWMutex
	outbound map[string]map[string]struct{}
}

func NewKeyGraph() *KeyGraph {
	return &KeyGraph{outbound: make(map[string]map[string]struct{})}
}

// Link adds a directed edge. Repeating the same edge is idempotent.
func (graph *KeyGraph) Link(from, to string) error {
	if graph == nil {
		return fmt.Errorf("key graph is nil")
	}
	if from == "" || to == "" {
		return fmt.Errorf("graph keys cannot be empty")
	}
	graph.mu.Lock()
	defer graph.mu.Unlock()
	if graph.outbound[from] == nil {
		graph.outbound[from] = make(map[string]struct{})
	}
	graph.outbound[from][to] = struct{}{}
	return nil
}

// Unlink removes a directed edge and reports whether it existed.
func (graph *KeyGraph) Unlink(from, to string) bool {
	if graph == nil || from == "" || to == "" {
		return false
	}
	graph.mu.Lock()
	defer graph.mu.Unlock()
	neighbors := graph.outbound[from]
	if _, ok := neighbors[to]; !ok {
		return false
	}
	delete(neighbors, to)
	if len(neighbors) == 0 {
		delete(graph.outbound, from)
	}
	return true
}

// Traverse visits root and reachable keys in deterministic breadth-first order.
func (graph *KeyGraph) Traverse(root string, maxDepth int) ([]GraphTraversal, error) {
	if graph == nil {
		return nil, fmt.Errorf("key graph is nil")
	}
	if root == "" {
		return nil, fmt.Errorf("graph root cannot be empty")
	}
	if maxDepth < 0 {
		return nil, fmt.Errorf("graph maximum depth cannot be negative")
	}
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	queue := []GraphTraversal{{Node: root}}
	visited := map[string]struct{}{root: {}}
	result := make([]GraphTraversal, 0, len(graph.outbound)+1)
	for head := 0; head < len(queue); head++ {
		current := queue[head]
		result = append(result, current)
		if current.Depth == maxDepth {
			continue
		}
		adjacent := graph.outbound[current.Node]
		if len(adjacent) == 1 {
			for neighbor := range adjacent {
				if _, seen := visited[neighbor]; !seen {
					visited[neighbor] = struct{}{}
					queue = append(queue, GraphTraversal{Node: neighbor, Depth: current.Depth + 1})
				}
			}
			continue
		}
		neighbors := graph.sortedNeighborsLocked(current.Node)
		for _, neighbor := range neighbors {
			if _, seen := visited[neighbor]; seen {
				continue
			}
			visited[neighbor] = struct{}{}
			queue = append(queue, GraphTraversal{Node: neighbor, Depth: current.Depth + 1})
		}
	}
	return result, nil
}

// ShortestPath finds the first deterministic breadth-first path from to to.
func (graph *KeyGraph) ShortestPath(from, to string, maxDepth int) ([]string, bool, error) {
	if graph == nil {
		return nil, false, fmt.Errorf("key graph is nil")
	}
	if from == "" || to == "" {
		return nil, false, fmt.Errorf("graph keys cannot be empty")
	}
	if maxDepth < 0 {
		return nil, false, fmt.Errorf("graph maximum depth cannot be negative")
	}
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	queue := []GraphTraversal{{Node: from}}
	parents := map[string]string{from: ""}
	for head := 0; head < len(queue); head++ {
		current := queue[head]
		if current.Node == to {
			return graph.pathLocked(parents, to), true, nil
		}
		if current.Depth == maxDepth {
			continue
		}
		adjacent := graph.outbound[current.Node]
		if len(adjacent) == 1 {
			for neighbor := range adjacent {
				if _, seen := parents[neighbor]; !seen {
					parents[neighbor] = current.Node
					queue = append(queue, GraphTraversal{Node: neighbor, Depth: current.Depth + 1})
				}
			}
			continue
		}
		for _, neighbor := range graph.sortedNeighborsLocked(current.Node) {
			if _, seen := parents[neighbor]; seen {
				continue
			}
			parents[neighbor] = current.Node
			queue = append(queue, GraphTraversal{Node: neighbor, Depth: current.Depth + 1})
		}
	}
	return nil, false, nil
}

func (graph *KeyGraph) sortedNeighborsLocked(node string) []string {
	neighbors := make([]string, 0, len(graph.outbound[node]))
	for neighbor := range graph.outbound[node] {
		neighbors = append(neighbors, neighbor)
	}
	sort.Strings(neighbors)
	return neighbors
}

func (graph *KeyGraph) pathLocked(parents map[string]string, target string) []string {
	path := make([]string, 0)
	for node := target; ; node = parents[node] {
		path = append(path, node)
		if parents[node] == "" {
			break
		}
	}
	for left, right := 0, len(path)-1; left < right; left, right = left+1, right-1 {
		path[left], path[right] = path[right], path[left]
	}
	return path
}
