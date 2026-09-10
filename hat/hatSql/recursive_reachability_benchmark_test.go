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
	withoutFirst := append([]recursiveReachabilityBenchmarkEdge(nil), initial[1:]...)
	withUpdate := append([]recursiveReachabilityBenchmarkEdge(nil), initial...)
	withUpdate[0].to = "leaf:new"

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
	b.Run("full_recompute_delete", func(b *testing.B) {
		for iteration := 0; iteration < b.N; iteration++ {
			if got := recursiveReachabilityBenchmarkClosure(withoutFirst); got != 2046 {
				b.Fatalf("closure size = %d, want 2046", got)
			}
		}
	})
	b.Run("full_recompute_update", func(b *testing.B) {
		for iteration := 0; iteration < b.N; iteration++ {
			if got := recursiveReachabilityBenchmarkClosure(withUpdate); got != 2048 {
				b.Fatalf("closure size = %d, want 2048", got)
			}
		}
	})
	b.Run("mutable_delete", func(b *testing.B) {
		seed := make([]RecursiveReachabilityEdge, len(initial))
		for index, edge := range initial {
			seed[index] = RecursiveReachabilityEdge{Key: edge.from + "->" + edge.to, From: edge.from, To: edge.to}
		}
		mutation := []RecursiveReachabilityMutation{{Kind: RecursiveReachabilityDelete, Key: "root:0->leaf:0"}}
		reachability := NewMutableIncrementalRecursiveReachability()
		if _, err := reachability.Append(seed); err != nil {
			b.Fatalf("Append(seed) error = %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			updates, err := reachability.Apply(mutation)
			if err != nil {
				b.Fatalf("Apply(delete) error = %v", err)
			}
			if len(updates) != 1 || updates[0].Diff != -1 {
				b.Fatalf("delete updates = %#v, want one negative pair", updates)
			}
			b.StopTimer()
			if _, err := reachability.Apply([]RecursiveReachabilityMutation{{
				Kind: RecursiveReachabilityInsert,
				Key:  "root:0->leaf:0",
				From: "root:0",
				To:   "leaf:0",
			}}); err != nil {
				b.Fatalf("Apply(insert restore) error = %v", err)
			}
			b.StartTimer()
		}
	})
	b.Run("mutable_update", func(b *testing.B) {
		seed := make([]RecursiveReachabilityEdge, len(initial))
		for index, edge := range initial {
			seed[index] = RecursiveReachabilityEdge{Key: edge.from + "->" + edge.to, From: edge.from, To: edge.to}
		}
		reachability := NewMutableIncrementalRecursiveReachability()
		if _, err := reachability.Append(seed); err != nil {
			b.Fatalf("Append(seed) error = %v", err)
		}
		mutation := RecursiveReachabilityMutation{Kind: RecursiveReachabilityUpdate, Key: "root:0->leaf:0", From: "root:0", To: "leaf:new"}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			updates, err := reachability.Apply([]RecursiveReachabilityMutation{mutation})
			if err != nil {
				b.Fatalf("Apply(update) error = %v", err)
			}
			if len(updates) != 2 {
				b.Fatalf("update updates = %#v, want one retraction and one insertion", updates)
			}
			b.StopTimer()
			mutation.To = "leaf:0"
			if _, err := reachability.Apply([]RecursiveReachabilityMutation{mutation}); err != nil {
				b.Fatalf("Apply(restore) error = %v", err)
			}
			mutation.To = "leaf:new"
			b.StartTimer()
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
