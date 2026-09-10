package hatSql

import (
	"strconv"
	"testing"
)

type recursiveReachabilityBenchmarkEdge struct {
	from string
	to   string
}

func BenchmarkRecursiveReachabilityMaintenance(b *testing.B) {
	initial := make([]recursiveReachabilityBenchmarkEdge, 0, 1024)
	for index := 0; index < 1024; index++ {
		initial = append(initial, recursiveReachabilityBenchmarkEdge{
			from: "root:" + strconv.Itoa(index),
			to:   "leaf:" + strconv.Itoa(index),
		})
	}
	withAppend := append(append([]recursiveReachabilityBenchmarkEdge(nil), initial...), recursiveReachabilityBenchmarkEdge{from: "root:0", to: "leaf:new"})

	b.Run("full_recompute", func(b *testing.B) {
		for iteration := 0; iteration < b.N; iteration++ {
			if got := recursiveReachabilityBenchmarkClosure(withAppend); got != 2049 {
				b.Fatalf("closure size = %d, want 2049", got)
			}
		}
	})
	b.Run("incremental_append", func(b *testing.B) {
		reachability := NewIncrementalRecursiveReachability()
		seed := make([]RecursiveReachabilityEdge, len(initial))
		for index, edge := range initial {
			seed[index] = RecursiveReachabilityEdge{Key: edge.from + "->" + edge.to, From: edge.from, To: edge.to}
		}
		if _, err := reachability.Append(seed); err != nil {
			b.Fatalf("Append(seed) error = %v", err)
		}
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			updates, err := reachability.Append([]RecursiveReachabilityEdge{{Key: "root:0->leaf:new:" + strconv.Itoa(iteration), From: "root:0", To: "leaf:new:" + strconv.Itoa(iteration)}})
			if err != nil {
				b.Fatalf("Append(tail) error = %v", err)
			}
			if len(updates) != 1 {
				b.Fatalf("tail updates = %#v, want one pair", updates)
			}
		}
	})
}

func recursiveReachabilityBenchmarkClosure(edges []recursiveReachabilityBenchmarkEdge) int {
	adjacency := make(map[string][]string, len(edges))
	for _, edge := range edges {
		adjacency[edge.from] = append(adjacency[edge.from], edge.to)
	}
	count := 0
	for source := range adjacency {
		seen := make(map[string]struct{})
		stack := []string{source}
		for len(stack) > 0 {
			last := len(stack) - 1
			node := stack[last]
			stack = stack[:last]
			if _, ok := seen[node]; ok {
				continue
			}
			seen[node] = struct{}{}
			count++
			stack = append(stack, adjacency[node]...)
		}
	}
	return count
}
