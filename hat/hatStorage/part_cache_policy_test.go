package hatStorage_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestPartCachePolicyAdmitsByAccessCountAndPlansDeterministicEvictions(t *testing.T) {
	policy, err := hatStorage.NewPartCachePolicy(100, 2)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Admit(hatStorage.PartCacheCandidate{Key: "part", SizeBytes: 10, Accesses: 1}) {
		t.Fatal("candidate below minimum accesses was admitted")
	}
	if !policy.Admit(hatStorage.PartCacheCandidate{Key: "part", SizeBytes: 10, Accesses: 2}) {
		t.Fatal("candidate at minimum accesses was rejected")
	}
	for name, candidate := range map[string]hatStorage.PartCacheCandidate{
		"empty key":  {SizeBytes: 10, Accesses: 2},
		"empty size": {Key: "part", Accesses: 2},
		"too large":  {Key: "part", SizeBytes: 101, Accesses: 2},
	} {
		if policy.Admit(candidate) {
			t.Errorf("%s candidate was admitted", name)
		}
	}

	entries := []hatStorage.PartCacheCandidate{
		{Key: "hot", SizeBytes: 50, Accesses: 10, LastAccess: 30},
		{Key: "cold", SizeBytes: 30, Accesses: 1, LastAccess: 10},
		{Key: "warm", SizeBytes: 40, Accesses: 2, LastAccess: 20},
	}
	plan, err := policy.PlanEvictions(entries, 40)
	if err != nil {
		t.Fatal(err)
	}
	expected := []hatStorage.PartCacheCandidate{entries[1], entries[2]}
	if !reflect.DeepEqual(plan, expected) {
		t.Fatalf("eviction plan = %#v, want %#v", plan, expected)
	}
	if !reflect.DeepEqual(entries, []hatStorage.PartCacheCandidate{
		{Key: "hot", SizeBytes: 50, Accesses: 10, LastAccess: 30},
		{Key: "cold", SizeBytes: 30, Accesses: 1, LastAccess: 10},
		{Key: "warm", SizeBytes: 40, Accesses: 2, LastAccess: 20},
	}) {
		t.Fatal("eviction planning mutated input entries")
	}
}

func TestPartCachePolicyRejectsInvalidCandidatesAndCapacity(t *testing.T) {
	if _, err := hatStorage.NewPartCachePolicy(0, 0); !errors.Is(err, hatStorage.ErrPartCachePolicyInvalid) {
		t.Fatalf("zero capacity error = %v", err)
	}
	policy, err := hatStorage.NewPartCachePolicy(100, 0)
	if err != nil {
		t.Fatal(err)
	}
	for name, entries := range map[string][]hatStorage.PartCacheCandidate{
		"empty key":  {{SizeBytes: 10}},
		"empty size": {{Key: "part"}},
		"duplicate":  {{Key: "part", SizeBytes: 10}, {Key: "part", SizeBytes: 20}},
	} {
		if _, err := policy.PlanEvictions(entries, 1); !errors.Is(err, hatStorage.ErrPartCacheCandidateInvalid) {
			t.Errorf("%s error = %v", name, err)
		}
	}
	if _, err := policy.PlanEvictions(nil, 101); !errors.Is(err, hatStorage.ErrPartCacheCapacityExceeded) {
		t.Fatalf("incoming over capacity error = %v", err)
	}
	if plan, err := policy.PlanEvictions(nil, 100); err != nil || plan != nil {
		t.Fatalf("empty eviction plan = %#v, %v", plan, err)
	}
}

func TestPartCachePolicyUsesStableTieBreakers(t *testing.T) {
	policy, err := hatStorage.NewPartCachePolicy(10, 0)
	if err != nil {
		t.Fatal(err)
	}
	entries := []hatStorage.PartCacheCandidate{
		{Key: "a", SizeBytes: 5, Accesses: 1, LastAccess: 1},
		{Key: "b", SizeBytes: 8, Accesses: 1, LastAccess: 1},
	}
	plan, err := policy.PlanEvictions(entries, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 1 || plan[0].Key != "b" {
		t.Fatalf("size tie-breaker plan = %#v, want b", plan)
	}

	entries = []hatStorage.PartCacheCandidate{
		{Key: "z", SizeBytes: 5, Accesses: 1, LastAccess: 1},
		{Key: "a", SizeBytes: 5, Accesses: 1, LastAccess: 1},
	}
	plan, err = policy.PlanEvictions(entries, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) != 1 || plan[0].Key != "a" {
		t.Fatalf("key tie-breaker plan = %#v, want a", plan)
	}
}

func BenchmarkPartCachePolicyAdmit(b *testing.B) {
	policy, err := hatStorage.NewPartCachePolicy(1<<30, 2)
	if err != nil {
		b.Fatal(err)
	}
	candidate := hatStorage.PartCacheCandidate{Key: "part-001", SizeBytes: 4096, Accesses: 3}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if !policy.Admit(candidate) {
			b.Fatal("candidate was unexpectedly rejected")
		}
	}
}
