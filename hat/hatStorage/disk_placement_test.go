package hatStorage_test

import (
	"errors"
	"testing"

	hatStorage "hatrie_cache/hat/hatStorage"
)

func TestDiskPlacementPolicySelectsDeterministicallyWithinRules(t *testing.T) {
	policy, err := hatStorage.NewDiskPlacementPolicy("hot-warm", []hatStorage.DiskPlacementRule{
		{Path: "/data/hot", Weight: 1},
		{Path: "/data/warm", Weight: 3},
	})
	if err != nil {
		t.Fatalf("NewDiskPlacementPolicy() error = %v", err)
	}
	first, err := policy.SelectPath("part-001")
	if err != nil {
		t.Fatalf("SelectPath() error = %v", err)
	}
	second, err := policy.SelectPath("part-001")
	if err != nil {
		t.Fatalf("SelectPath() repeated error = %v", err)
	}
	if first != second || (first != "/data/hot" && first != "/data/warm") {
		t.Fatalf("SelectPath() = %q then %q, want deterministic configured path", first, second)
	}

	rules := policy.Rules()
	if len(rules) != 2 || rules[0].Path != "/data/hot" || rules[1].Weight != 3 {
		t.Fatalf("Rules() = %#v", rules)
	}
	rules[0].Path = "/mutated"
	unchanged, err := policy.SelectPath("part-001")
	if err != nil {
		t.Fatalf("SelectPath() after Rules mutation error = %v", err)
	}
	if unchanged != first {
		t.Fatalf("SelectPath() changed after Rules mutation: %q, want %q", unchanged, first)
	}
}

func TestDiskPlacementPolicyRejectsInvalidRules(t *testing.T) {
	cases := []struct {
		name  string
		name2 string
		rules []hatStorage.DiskPlacementRule
	}{
		{name: "missing name", rules: []hatStorage.DiskPlacementRule{{Path: "/data", Weight: 1}}},
		{name: "empty rules", name2: "policy", rules: nil},
		{name: "empty path", name2: "policy", rules: []hatStorage.DiskPlacementRule{{Weight: 1}}},
		{name: "zero weight", name2: "policy", rules: []hatStorage.DiskPlacementRule{{Path: "/data", Weight: 0}}},
		{name: "duplicate path", name2: "policy", rules: []hatStorage.DiskPlacementRule{{Path: "/data", Weight: 1}, {Path: "/data", Weight: 1}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := hatStorage.NewDiskPlacementPolicy(test.name2, test.rules); !errors.Is(err, hatStorage.ErrDiskPlacementPolicyInvalid) {
				t.Fatalf("NewDiskPlacementPolicy() error = %v, want ErrDiskPlacementPolicyInvalid", err)
			}
		})
	}
}

func TestDiskPlacementPolicyRejectsSelectionWithoutRules(t *testing.T) {
	var policy hatStorage.DiskPlacementPolicy
	if _, err := policy.SelectPath("part"); !errors.Is(err, hatStorage.ErrDiskPlacementUnavailable) {
		t.Fatalf("SelectPath() error = %v, want ErrDiskPlacementUnavailable", err)
	}
}

func BenchmarkDiskPlacementPolicySelectPath(b *testing.B) {
	policy, err := hatStorage.NewDiskPlacementPolicy("policy", []hatStorage.DiskPlacementRule{
		{Path: "/data/a", Weight: 1},
		{Path: "/data/b", Weight: 2},
		{Path: "/data/c", Weight: 1},
	})
	if err != nil {
		b.Fatal(err)
	}
	keys := [...]string{"part-0", "part-1", "part-2", "part-3"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := range b.N {
		if _, err := policy.SelectPath(keys[index%len(keys)]); err != nil {
			b.Fatal(err)
		}
	}
}
