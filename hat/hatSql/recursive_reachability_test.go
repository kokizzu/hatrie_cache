package hatSql

import (
	"errors"
	"testing"
)

func TestIncrementalRecursiveReachabilityEmitsNewTransitivePairs(t *testing.T) {
	reachability := NewIncrementalRecursiveReachability()
	updates, err := reachability.Append([]RecursiveReachabilityEdge{
		{Key: "ab", From: "a", To: "b"},
		{Key: "bc", From: "b", To: "c"},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if len(updates) != 3 {
		t.Fatalf("first updates = %#v, want three pairs", updates)
	}
	for _, pair := range []struct{ from, to string }{{"a", "b"}, {"b", "c"}, {"a", "c"}} {
		if !reachability.Reachable(pair.from, pair.to) {
			t.Fatalf("Reachable(%q, %q) = false, want true", pair.from, pair.to)
		}
	}

	updates, err = reachability.Append([]RecursiveReachabilityEdge{{Key: "cd", From: "c", To: "d"}})
	if err != nil {
		t.Fatalf("Append(transitive tail) error = %v", err)
	}
	if len(updates) != 3 {
		t.Fatalf("tail updates = %#v, want three new pairs", updates)
	}
	for _, pair := range []struct{ from, to string }{{"a", "d"}, {"b", "d"}, {"c", "d"}} {
		if !reachability.Reachable(pair.from, pair.to) {
			t.Fatalf("Reachable(%q, %q) = false after tail", pair.from, pair.to)
		}
	}
	if reachability.Reachable("d", "a") {
		t.Fatal("unexpected reverse reachability")
	}
}

func TestIncrementalRecursiveReachabilityHandlesCyclesWithoutDuplicatePairs(t *testing.T) {
	reachability := NewIncrementalRecursiveReachability()
	if _, err := reachability.Append([]RecursiveReachabilityEdge{
		{Key: "ab", From: "a", To: "b"},
		{Key: "bc", From: "b", To: "c"},
	}); err != nil {
		t.Fatalf("Append(initial) error = %v", err)
	}
	updates, err := reachability.Append([]RecursiveReachabilityEdge{{Key: "ca", From: "c", To: "a"}})
	if err != nil {
		t.Fatalf("Append(cycle) error = %v", err)
	}
	if len(updates) != 6 {
		t.Fatalf("cycle updates = %#v, want six new pairs", updates)
	}
	for _, from := range []string{"a", "b", "c"} {
		for _, to := range []string{"a", "b", "c"} {
			if !reachability.Reachable(from, to) {
				t.Fatalf("Reachable(%q, %q) = false in cycle", from, to)
			}
		}
	}
}

func TestIncrementalRecursiveReachabilityValidatesBatchAtomically(t *testing.T) {
	reachability := NewIncrementalRecursiveReachability()
	if _, err := reachability.Append([]RecursiveReachabilityEdge{{Key: "ab", From: "a", To: "b"}}); err != nil {
		t.Fatalf("Append(initial) error = %v", err)
	}
	_, err := reachability.Append([]RecursiveReachabilityEdge{
		{Key: "bc", From: "b", To: "c"},
		{Key: "ab", From: "a", To: "b"},
	})
	if !errors.Is(err, ErrIncrementalRecursiveReachabilityDuplicateEdge) {
		t.Fatalf("Append(duplicate) error = %v, want duplicate edge error", err)
	}
	if reachability.Reachable("b", "c") {
		t.Fatal("failed batch published b->c")
	}

	_, err = reachability.Append([]RecursiveReachabilityEdge{{Key: "bad", From: "b", To: ""}})
	if !errors.Is(err, ErrIncrementalRecursiveReachabilityNodeRequired) {
		t.Fatalf("Append(invalid node) error = %v, want node error", err)
	}
	if reachability.Reachable("b", "") {
		t.Fatal("invalid empty node became reachable")
	}
}

func TestIncrementalRecursiveReachabilityRejectsNilReceiver(t *testing.T) {
	var reachability *IncrementalRecursiveReachability
	if _, err := reachability.Append([]RecursiveReachabilityEdge{{Key: "ab", From: "a", To: "b"}}); !errors.Is(err, ErrIncrementalRecursiveReachabilityNil) {
		t.Fatalf("Append(nil) error = %v, want nil receiver error", err)
	}
}
