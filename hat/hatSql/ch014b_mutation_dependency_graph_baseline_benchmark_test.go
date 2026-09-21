package hatSql_test

import (
	"fmt"
	"sort"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const ch014bBenchmarkBlockedTasks = 4096

func BenchmarkCH014BExistingClaimReadyNoReady(b *testing.B) {
	graph := newCH014BLegacyBlockedGraph()
	b.ResetTimer()
	for range b.N {
		if claimed := ch014BLegacyClaimReady(graph, 1); len(claimed) != 0 {
			b.Fatalf("ClaimReady returned %d blocked tasks", len(claimed))
		}
	}
}

type ch014BLegacyTask struct {
	state      uint8
	dependents []string
}

func newCH014BLegacyBlockedGraph() map[string]ch014BLegacyTask {
	graph := make(map[string]ch014BLegacyTask, ch014bBenchmarkBlockedTasks+1)
	graph["root"] = ch014BLegacyTask{state: 2}
	for index := 0; index < ch014bBenchmarkBlockedTasks; index++ {
		graph[ch014bBenchmarkTaskID(index)] = ch014BLegacyTask{dependents: []string{"root"}}
	}
	return graph
}

func ch014BLegacyClaimReady(graph map[string]ch014BLegacyTask, limit int) []string {
	ids := make([]string, 0, len(graph))
	for id, task := range graph {
		if task.state != 0 {
			continue
		}
		ready := true
		for _, dependency := range task.dependents {
			if graph[dependency].state != 3 {
				ready = false
				break
			}
		}
		if ready {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return ids
}

func newCH014BBlockedGraph() (*hatSql.SQLMutationDependencyGraph, error) {
	graph, err := hatSql.NewSQLMutationDependencyGraph(ch014bBenchmarkBlockedTasks + 1)
	if err != nil {
		return nil, err
	}
	if err := graph.Add(hatSql.SQLMutationTask{ID: "root"}); err != nil {
		return nil, err
	}
	for index := 0; index < ch014bBenchmarkBlockedTasks; index++ {
		if err := graph.Add(hatSql.SQLMutationTask{ID: ch014bBenchmarkTaskID(index), DependsOn: []string{"root"}}); err != nil {
			return nil, err
		}
	}
	claimed := graph.ClaimReady(1)
	if len(claimed) != 1 || claimed[0].ID != "root" {
		return nil, fmt.Errorf("initial ClaimReady = %#v", claimed)
	}
	return graph, nil
}

func ch014bBenchmarkTaskID(index int) string {
	return fmt.Sprintf("task-%04d", index)
}
