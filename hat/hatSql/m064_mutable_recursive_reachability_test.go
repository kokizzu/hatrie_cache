package hatSql

import (
	"errors"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"
)

func TestMutableRecursiveReachabilityAppliesRetractionsAndUpdates(t *testing.T) {
	reachability := NewMutableIncrementalRecursiveReachability()
	if _, err := reachability.Append([]RecursiveReachabilityEdge{
		{Key: "ab", From: "a", To: "b"},
		{Key: "bc", From: "b", To: "c"},
		{Key: "cd", From: "c", To: "d"},
	}); err != nil {
		t.Fatal(err)
	}
	updates, err := reachability.Apply([]RecursiveReachabilityMutation{{
		Kind: RecursiveReachabilityDelete,
		Key:  "bc",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 4 {
		t.Fatalf("delete updates = %#v, want four retractions", updates)
	}
	for _, update := range updates {
		if update.Diff != -1 {
			t.Fatalf("delete update = %#v, want negative diff", update)
		}
	}
	for _, pair := range []struct{ from, to string }{{"a", "c"}, {"a", "d"}, {"b", "c"}, {"b", "d"}} {
		if reachability.Reachable(pair.from, pair.to) {
			t.Fatalf("Reachable(%q, %q) = true after delete", pair.from, pair.to)
		}
	}
	if !reachability.Reachable("a", "b") || !reachability.Reachable("c", "d") {
		t.Fatal("unaffected reachability was lost")
	}

	updates, err = reachability.Apply([]RecursiveReachabilityMutation{{
		Kind: RecursiveReachabilityInsert,
		Key:  "bc",
		From: "b",
		To:   "c",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 4 {
		t.Fatalf("reinsert updates = %#v, want four insertions", updates)
	}
	for _, update := range updates {
		if update.Diff != 1 {
			t.Fatalf("reinsert update = %#v, want positive diff", update)
		}
	}

	updates, err = reachability.Apply([]RecursiveReachabilityMutation{{
		Kind: RecursiveReachabilityUpdate,
		Key:  "cd",
		From: "c",
		To:   "x",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 6 {
		t.Fatalf("edge update = %#v, want c->d and its a/b transitive consequences replaced", updates)
	}
	for _, update := range updates {
		if update.Diff != -1 && update.Diff != 1 {
			t.Fatalf("edge update = %#v, want signed differential", update)
		}
	}
	if reachability.Reachable("a", "d") || reachability.Reachable("b", "d") || !reachability.Reachable("a", "x") || !reachability.Reachable("b", "x") {
		t.Fatal("updated closure is incorrect")
	}
}

func TestMutableRecursiveReachabilityHandlesCycleRetractions(t *testing.T) {
	reachability := NewMutableIncrementalRecursiveReachability()
	if _, err := reachability.Append([]RecursiveReachabilityEdge{
		{Key: "ab", From: "a", To: "b"},
		{Key: "bc", From: "b", To: "c"},
		{Key: "ca", From: "c", To: "a"},
	}); err != nil {
		t.Fatal(err)
	}
	updates, err := reachability.Apply([]RecursiveReachabilityMutation{{
		Kind: RecursiveReachabilityDelete,
		Key:  "ca",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 6 {
		t.Fatalf("cycle delete updates = %#v, want six retractions", updates)
	}
	for _, pair := range []struct{ from, to string }{{"a", "a"}, {"b", "b"}, {"c", "c"}, {"b", "a"}, {"c", "a"}, {"c", "b"}} {
		if reachability.Reachable(pair.from, pair.to) {
			t.Fatalf("Reachable(%q, %q) = true after cycle removal", pair.from, pair.to)
		}
	}
	if !reachability.Reachable("a", "b") || !reachability.Reachable("b", "c") {
		t.Fatal("remaining forward edges were lost")
	}
}

func TestMutableRecursiveReachabilityAppliesMixedBatchAtomically(t *testing.T) {
	reachability := NewMutableIncrementalRecursiveReachability()
	if _, err := reachability.Append([]RecursiveReachabilityEdge{
		{Key: "ab", From: "a", To: "b"},
		{Key: "bc", From: "b", To: "c"},
		{Key: "cd", From: "c", To: "d"},
	}); err != nil {
		t.Fatal(err)
	}
	updates, err := reachability.Apply([]RecursiveReachabilityMutation{
		{Kind: RecursiveReachabilityDelete, Key: "bc"},
		{Kind: RecursiveReachabilityInsert, Key: "bd", From: "b", To: "d"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 2 {
		t.Fatalf("mixed batch updates = %#v, want two retractions", updates)
	}
	for _, update := range updates {
		if update.Diff != -1 || (update.Key != recursiveReachabilityPairKey("a", "c") && update.Key != recursiveReachabilityPairKey("b", "c")) {
			t.Fatalf("mixed batch update = %#v, want a->c and b->c retractions", update)
		}
	}
	if !reachability.Reachable("a", "d") || !reachability.Reachable("b", "d") || reachability.Reachable("a", "c") || reachability.Reachable("b", "c") {
		t.Fatal("mixed batch closure is incorrect")
	}
}

func TestMutableRecursiveReachabilityMutationValidationIsAtomic(t *testing.T) {
	reachability := NewMutableIncrementalRecursiveReachability()
	if _, err := reachability.Append([]RecursiveReachabilityEdge{{Key: "ab", From: "a", To: "b"}}); err != nil {
		t.Fatal(err)
	}
	for name, mutations := range map[string][]RecursiveReachabilityMutation{
		"duplicate batch key": {
			{Kind: RecursiveReachabilityInsert, Key: "bc", From: "b", To: "c"},
			{Kind: RecursiveReachabilityInsert, Key: "bc", From: "b", To: "d"},
		},
		"missing delete": {{Kind: RecursiveReachabilityDelete, Key: "missing"}},
		"missing update": {{Kind: RecursiveReachabilityUpdate, Key: "missing", From: "m", To: "n"}},
		"invalid node":   {{Kind: RecursiveReachabilityInsert, Key: "bad", From: "", To: "c"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := reachability.Apply(mutations); err == nil {
				t.Fatal("Apply unexpectedly succeeded")
			}
		})
	}
	if reachability.Reachable("b", "c") {
		t.Fatal("failed mutation batch changed closure")
	}
	updates, err := reachability.Apply([]RecursiveReachabilityMutation{{
		Kind: RecursiveReachabilityInsert,
		Key:  "bc",
		From: "b",
		To:   "c",
	}})
	if err != nil || len(updates) != 2 {
		t.Fatalf("post-validation insert = %#v, %v", updates, err)
	}
}

func TestIncrementalRecursiveReachabilityApplyRequiresOptIn(t *testing.T) {
	reachability := NewIncrementalRecursiveReachability()
	_, err := reachability.Apply([]RecursiveReachabilityMutation{{
		Kind: RecursiveReachabilityInsert,
		Key:  "ab",
		From: "a",
		To:   "b",
	}})
	if !errors.Is(err, ErrIncrementalRecursiveReachabilityMutationsDisabled) {
		t.Fatalf("Apply error = %v, want %v", err, ErrIncrementalRecursiveReachabilityMutationsDisabled)
	}
}

func TestMutableRecursiveReachabilityMatchesNaiveClosureAcrossMixedMutations(t *testing.T) {
	reachability := NewMutableIncrementalRecursiveReachability()
	edges := make(map[string]RecursiveReachabilityEdge)
	nodes := make(map[string]struct{})
	random := rand.New(rand.NewSource(64064))
	nodeNames := []string{"a", "b", "c", "d", "e"}
	for iteration := 0; iteration < 160; iteration++ {
		before := naiveRecursiveReachabilityClosure(edges)
		var mutation RecursiveReachabilityMutation
		if len(edges) == 0 || random.Intn(100) < 45 {
			key := "edge-" + strconv.Itoa(iteration)
			mutation = RecursiveReachabilityMutation{
				Kind: RecursiveReachabilityInsert,
				Key:  key,
				From: nodeNames[random.Intn(len(nodeNames))],
				To:   nodeNames[random.Intn(len(nodeNames))],
			}
			edges[key] = RecursiveReachabilityEdge{Key: key, From: mutation.From, To: mutation.To}
		} else {
			keys := make([]string, 0, len(edges))
			for key := range edges {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			key := keys[random.Intn(len(keys))]
			if random.Intn(100) < 55 {
				mutation = RecursiveReachabilityMutation{Kind: RecursiveReachabilityUpdate, Key: key, From: nodeNames[random.Intn(len(nodeNames))], To: nodeNames[random.Intn(len(nodeNames))]}
				edges[key] = RecursiveReachabilityEdge{Key: key, From: mutation.From, To: mutation.To}
			} else {
				mutation = RecursiveReachabilityMutation{Kind: RecursiveReachabilityDelete, Key: key}
				delete(edges, key)
			}
		}
		if mutation.Kind != RecursiveReachabilityDelete {
			nodes[mutation.From] = struct{}{}
			nodes[mutation.To] = struct{}{}
		}

		updates, err := reachability.Apply([]RecursiveReachabilityMutation{mutation})
		if err != nil {
			t.Fatalf("iteration %d Apply(%#v) error = %v", iteration, mutation, err)
		}
		after := naiveRecursiveReachabilityClosure(edges)
		wantChanges := naiveRecursiveReachabilityChanges(before, after, nodes)
		gotChanges := make(map[string]int64, len(updates))
		for _, update := range updates {
			gotChanges[update.Key] += update.Diff
		}
		if !reflect.DeepEqual(gotChanges, wantChanges) {
			t.Fatalf("iteration %d changes = %#v, want %#v", iteration, gotChanges, wantChanges)
		}
		for from := range nodes {
			for to := range nodes {
				_, want := after[from][to]
				if got := reachability.Reachable(from, to); got != want {
					t.Fatalf("iteration %d Reachable(%q, %q) = %v, want %v", iteration, from, to, got, want)
				}
			}
		}
	}
}

func naiveRecursiveReachabilityClosure(edges map[string]RecursiveReachabilityEdge) map[string]map[string]struct{} {
	adjacency := make(map[string][]string)
	nodes := make(map[string]struct{})
	for _, edge := range edges {
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
		nodes[edge.From] = struct{}{}
		nodes[edge.To] = struct{}{}
	}
	closure := make(map[string]map[string]struct{})
	for source := range nodes {
		visited := map[string]struct{}{source: {}}
		stack := append([]string(nil), adjacency[source]...)
		for len(stack) > 0 {
			last := len(stack) - 1
			node := stack[last]
			stack = stack[:last]
			if node == source {
				if closure[source] == nil {
					closure[source] = make(map[string]struct{})
				}
				closure[source][node] = struct{}{}
				continue
			}
			if _, exists := visited[node]; exists {
				continue
			}
			visited[node] = struct{}{}
			if closure[source] == nil {
				closure[source] = make(map[string]struct{})
			}
			closure[source][node] = struct{}{}
			stack = append(stack, adjacency[node]...)
		}
	}
	return closure
}

func naiveRecursiveReachabilityChanges(before, after map[string]map[string]struct{}, nodes map[string]struct{}) map[string]int64 {
	changes := make(map[string]int64)
	for from := range nodes {
		for to := range nodes {
			_, had := before[from][to]
			_, has := after[from][to]
			if had == has {
				continue
			}
			key := recursiveReachabilityPairKey(from, to)
			if has {
				changes[key] = 1
			} else {
				changes[key] = -1
			}
		}
	}
	return changes
}
