package hatSql

import (
	"errors"
	"testing"
)

func TestMutableRecursiveReachabilityApplyWithOptionsReportsConvergence(t *testing.T) {
	reachability := NewMutableIncrementalRecursiveReachability()
	if _, err := reachability.Apply([]RecursiveReachabilityMutation{
		{Kind: RecursiveReachabilityInsert, Key: "ab", From: "a", To: "b"},
		{Kind: RecursiveReachabilityInsert, Key: "bc", From: "b", To: "c"},
		{Kind: RecursiveReachabilityInsert, Key: "cd", From: "c", To: "d"},
	}); err != nil {
		t.Fatal(err)
	}

	updates, stats, err := reachability.ApplyWithOptions(
		[]RecursiveReachabilityMutation{{Kind: RecursiveReachabilityInsert, Key: "de", From: "d", To: "e"}},
		RecursiveReachabilityApplyOptions{MaxAffectedSources: 4, MaxIterations: 4, MaxTraversalSteps: 20, MaxEmittedUpdates: 4},
	)
	if err != nil {
		t.Fatalf("ApplyWithOptions() error = %v", err)
	}
	if len(updates) != 4 || stats.AddedPairs != 4 || stats.RemovedPairs != 0 || stats.EmittedUpdates != 4 {
		t.Fatalf("ApplyWithOptions() updates/stats = %d/%+v, want 4 added updates", len(updates), stats)
	}
	if stats.MutationCount != 1 || stats.AffectedSources != 4 || stats.MaxIterations != 4 || stats.TraversalSteps == 0 {
		t.Fatalf("ApplyWithOptions() stats = %+v, want mutation=1 affected=4 iterations=4 and traversal work", stats)
	}
	if !reachability.Reachable("a", "e") {
		t.Fatal("ApplyWithOptions() did not publish the new transitive pair")
	}
}

func TestMutableRecursiveReachabilityApplyWithOptionsUsesInsertFastPath(t *testing.T) {
	reachability := NewMutableIncrementalRecursiveReachability()
	if _, err := reachability.Apply([]RecursiveReachabilityMutation{
		{Kind: RecursiveReachabilityInsert, Key: "ab", From: "a", To: "b"},
		{Kind: RecursiveReachabilityInsert, Key: "bc", From: "b", To: "c"},
		{Kind: RecursiveReachabilityInsert, Key: "cd", From: "c", To: "d"},
	}); err != nil {
		t.Fatal(err)
	}
	updates, stats, err := reachability.ApplyWithOptions(
		[]RecursiveReachabilityMutation{{Kind: RecursiveReachabilityInsert, Key: "de", From: "d", To: "e"}},
		RecursiveReachabilityApplyOptions{MaxAffectedSources: 4, MaxEmittedUpdates: 4},
	)
	if err != nil {
		t.Fatalf("ApplyWithOptions() error = %v", err)
	}
	if len(updates) != 4 || stats.AddedPairs != 4 || stats.AffectedSources != 4 || stats.TraversalSteps != 0 {
		t.Fatalf("ApplyWithOptions() updates/stats = %d/%+v, want four updates, four sources, and no traversal walk", len(updates), stats)
	}
}

func TestMutableRecursiveReachabilityApplyWithOptionsRejectsBeforeMutation(t *testing.T) {
	reachability := NewMutableIncrementalRecursiveReachability()
	if _, err := reachability.Apply([]RecursiveReachabilityMutation{
		{Kind: RecursiveReachabilityInsert, Key: "ab", From: "a", To: "b"},
		{Kind: RecursiveReachabilityInsert, Key: "bc", From: "b", To: "c"},
		{Kind: RecursiveReachabilityInsert, Key: "cd", From: "c", To: "d"},
	}); err != nil {
		t.Fatal(err)
	}

	_, stats, err := reachability.ApplyWithOptions(
		[]RecursiveReachabilityMutation{{Kind: RecursiveReachabilityInsert, Key: "de", From: "d", To: "e"}},
		RecursiveReachabilityApplyOptions{MaxAffectedSources: 3},
	)
	if !errors.Is(err, ErrIncrementalRecursiveReachabilityLimitExceeded) {
		t.Fatalf("ApplyWithOptions() error = %v, want limit error", err)
	}
	if stats.AffectedSources != 4 {
		t.Fatalf("ApplyWithOptions() stats = %+v, want observed affected source count", stats)
	}
	if reachability.Reachable("a", "e") {
		t.Fatal("ApplyWithOptions() changed reachability after a rejected limit")
	}
	if _, err := reachability.Apply([]RecursiveReachabilityMutation{{Kind: RecursiveReachabilityInsert, Key: "de", From: "d", To: "e"}}); err != nil {
		t.Fatalf("Apply() after rejected bounded apply = %v", err)
	}
}

func TestMutableRecursiveReachabilityApplyWithOptionsRejectsIterationLimit(t *testing.T) {
	reachability := NewMutableIncrementalRecursiveReachability()
	if _, err := reachability.Apply([]RecursiveReachabilityMutation{
		{Kind: RecursiveReachabilityInsert, Key: "ab", From: "a", To: "b"},
		{Kind: RecursiveReachabilityInsert, Key: "bc", From: "b", To: "c"},
		{Kind: RecursiveReachabilityInsert, Key: "cd", From: "c", To: "d"},
	}); err != nil {
		t.Fatal(err)
	}

	_, stats, err := reachability.ApplyWithOptions(
		[]RecursiveReachabilityMutation{{Kind: RecursiveReachabilityInsert, Key: "de", From: "d", To: "e"}},
		RecursiveReachabilityApplyOptions{MaxIterations: 3},
	)
	if !errors.Is(err, ErrIncrementalRecursiveReachabilityLimitExceeded) {
		t.Fatalf("ApplyWithOptions() error = %v, want iteration limit error", err)
	}
	if stats.MaxIterations != 4 {
		t.Fatalf("ApplyWithOptions() stats = %+v, want observed depth 4", stats)
	}
	if reachability.Reachable("a", "e") {
		t.Fatal("ApplyWithOptions() changed reachability after an iteration-limit rejection")
	}
}

func TestMutableRecursiveReachabilityApplyWithOptionsMatchesApply(t *testing.T) {
	withOptions := NewMutableIncrementalRecursiveReachability()
	legacy := NewMutableIncrementalRecursiveReachability()
	seed := []RecursiveReachabilityMutation{
		{Kind: RecursiveReachabilityInsert, Key: "ab", From: "a", To: "b"},
		{Kind: RecursiveReachabilityInsert, Key: "bc", From: "b", To: "c"},
		{Kind: RecursiveReachabilityInsert, Key: "cd", From: "c", To: "d"},
		{Kind: RecursiveReachabilityInsert, Key: "db", From: "d", To: "b"},
		{Kind: RecursiveReachabilityInsert, Key: "xb", From: "x", To: "b"},
	}
	if _, err := withOptions.Apply(seed); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Apply(seed); err != nil {
		t.Fatal(err)
	}
	for _, mutations := range [][]RecursiveReachabilityMutation{
		{{Kind: RecursiveReachabilityInsert, Key: "de", From: "d", To: "e"}},
		{{Kind: RecursiveReachabilityUpdate, Key: "bc", From: "b", To: "x"}},
		{{Kind: RecursiveReachabilityDelete, Key: "db"}},
		{{Kind: RecursiveReachabilityInsert, Key: "yb", From: "y", To: "b"}},
	} {
		if _, _, err := withOptions.ApplyWithOptions(mutations, RecursiveReachabilityApplyOptions{}); err != nil {
			t.Fatalf("ApplyWithOptions(%+v) error = %v", mutations, err)
		}
		if _, err := legacy.Apply(mutations); err != nil {
			t.Fatalf("Apply(%+v) error = %v", mutations, err)
		}
		assertRecursiveReachabilityStateEqual(t, withOptions, legacy)
	}
}

func TestMutableRecursiveReachabilityApplyWithOptionsRejectsInvalidBudget(t *testing.T) {
	reachability := NewMutableIncrementalRecursiveReachability()
	_, _, err := reachability.ApplyWithOptions(nil, RecursiveReachabilityApplyOptions{MaxIterations: -1})
	if !errors.Is(err, ErrIncrementalRecursiveReachabilityOptionsInvalid) {
		t.Fatalf("ApplyWithOptions() error = %v, want invalid-options error", err)
	}
	if _, _, err := NewIncrementalRecursiveReachability().ApplyWithOptions(nil, RecursiveReachabilityApplyOptions{}); !errors.Is(err, ErrIncrementalRecursiveReachabilityMutationsDisabled) {
		t.Fatalf("append-only ApplyWithOptions() error = %v, want disabled error", err)
	}
}

func TestMutableRecursiveReachabilityApplyWithOptionsRejectsEmittedUpdateLimit(t *testing.T) {
	reachability := NewMutableIncrementalRecursiveReachability()
	if _, err := reachability.Apply([]RecursiveReachabilityMutation{
		{Kind: RecursiveReachabilityInsert, Key: "ab", From: "a", To: "b"},
		{Kind: RecursiveReachabilityInsert, Key: "bc", From: "b", To: "c"},
		{Kind: RecursiveReachabilityInsert, Key: "cd", From: "c", To: "d"},
	}); err != nil {
		t.Fatal(err)
	}
	_, stats, err := reachability.ApplyWithOptions(
		[]RecursiveReachabilityMutation{{Kind: RecursiveReachabilityInsert, Key: "de", From: "d", To: "e"}},
		RecursiveReachabilityApplyOptions{MaxEmittedUpdates: 3},
	)
	if !errors.Is(err, ErrIncrementalRecursiveReachabilityLimitExceeded) {
		t.Fatalf("ApplyWithOptions() error = %v, want emitted-update limit error", err)
	}
	if stats.EmittedUpdates != 4 || stats.AddedPairs+stats.RemovedPairs != 4 {
		t.Fatalf("ApplyWithOptions() stats = %+v, want observed fourth update", stats)
	}
	if reachability.Reachable("a", "e") {
		t.Fatal("ApplyWithOptions() changed reachability after an emitted-update rejection")
	}
}

func assertRecursiveReachabilityStateEqual(t *testing.T, left, right *IncrementalRecursiveReachability) {
	t.Helper()
	if len(left.edges) != len(right.edges) || len(left.reachable) != len(right.reachable) || len(left.ancestors) != len(right.ancestors) {
		t.Fatalf("state sizes differ: left edges/reachable/ancestors=%d/%d/%d right=%d/%d/%d", len(left.edges), len(left.reachable), len(left.ancestors), len(right.edges), len(right.reachable), len(right.ancestors))
	}
	for key := range left.edges {
		if _, exists := right.edges[key]; !exists {
			t.Fatalf("right state is missing edge %q", key)
		}
	}
	for source, destinations := range left.reachable {
		if len(destinations) != len(right.reachable[source]) {
			t.Fatalf("reachable[%q] sizes differ: left=%d right=%d", source, len(destinations), len(right.reachable[source]))
		}
		for destination := range destinations {
			if _, exists := right.reachable[source][destination]; !exists {
				t.Fatalf("right state is missing reachable pair %q -> %q", source, destination)
			}
		}
	}
}
