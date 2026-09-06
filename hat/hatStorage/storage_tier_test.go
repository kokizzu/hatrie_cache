package hatStorage_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func TestStorageTierPolicySelectsTierByAgeAndPath(t *testing.T) {
	hot, err := hatStorage.NewDiskPlacementPolicy("hot", []hatStorage.DiskPlacementRule{{Path: "/data/hot", Weight: 1}})
	if err != nil {
		t.Fatal(err)
	}
	warm, err := hatStorage.NewDiskPlacementPolicy("warm", []hatStorage.DiskPlacementRule{{Path: "/data/warm", Weight: 1}})
	if err != nil {
		t.Fatal(err)
	}
	cold, err := hatStorage.NewDiskPlacementPolicy("cold", []hatStorage.DiskPlacementRule{{Path: "/data/cold", Weight: 1}})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := hatStorage.NewStorageTierPolicy([]hatStorage.StorageTierRule{
		{Name: "cold", MinAge: 24 * time.Hour, Placement: cold},
		{Name: "hot", MinAge: 0, Placement: hot},
		{Name: "warm", MinAge: time.Hour, Placement: warm},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		age  time.Duration
		name string
		path string
	}{
		{age: 30 * time.Minute, name: "hot", path: "/data/hot"},
		{age: 2 * time.Hour, name: "warm", path: "/data/warm"},
		{age: 48 * time.Hour, name: "cold", path: "/data/cold"},
	} {
		selection, err := policy.Select(test.age, "part-001")
		if err != nil {
			t.Fatal(err)
		}
		if selection.Tier != test.name || selection.Path != test.path {
			t.Fatalf("selection at %s = %#v, want tier=%q path=%q", test.age, selection, test.name, test.path)
		}
	}
	first, err := policy.Select(2*time.Hour, "part-001")
	if err != nil {
		t.Fatal(err)
	}
	second, err := policy.Select(2*time.Hour, "part-001")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("selection is not deterministic: %#v != %#v", first, second)
	}

	rules := policy.Rules()
	rules[0].Name = "changed"
	if policy.Rules()[0].Name == "changed" {
		t.Fatal("rules snapshot mutation changed policy")
	}
}

func TestStorageTierPolicyRejectsInvalidRulesAndAge(t *testing.T) {
	placement, err := hatStorage.NewDiskPlacementPolicy("valid", []hatStorage.DiskPlacementRule{{Path: "/data", Weight: 1}})
	if err != nil {
		t.Fatal(err)
	}
	valid := hatStorage.StorageTierRule{Name: "hot", MinAge: 0, Placement: placement}
	for name, rules := range map[string][]hatStorage.StorageTierRule{
		"empty":          nil,
		"missing zero":   {{Name: "warm", MinAge: time.Hour, Placement: placement}},
		"negative age":   {{Name: "hot", MinAge: -time.Second, Placement: placement}},
		"duplicate name": {valid, {Name: "hot", MinAge: time.Hour, Placement: placement}},
		"duplicate age":  {valid, {Name: "warm", MinAge: 0, Placement: placement}},
		"empty name":     {{Name: " ", MinAge: 0, Placement: placement}},
	} {
		if _, err := hatStorage.NewStorageTierPolicy(rules); !errors.Is(err, hatStorage.ErrStorageTierPolicyInvalid) {
			t.Errorf("%s error = %v", name, err)
		}
	}

	policy, err := hatStorage.NewStorageTierPolicy([]hatStorage.StorageTierRule{valid})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := policy.Select(-time.Second, "part"); !errors.Is(err, hatStorage.ErrStorageTierUnavailable) {
		t.Fatalf("negative age error = %v", err)
	}
	if _, err := policy.Select(time.Hour, "part"); err != nil {
		t.Fatalf("single tier should cover all ages: %v", err)
	}
}

func BenchmarkStorageTierPolicySelect(b *testing.B) {
	placement, err := hatStorage.NewDiskPlacementPolicy("bench", []hatStorage.DiskPlacementRule{{Path: "/data", Weight: 1}})
	if err != nil {
		b.Fatal(err)
	}
	policy, err := hatStorage.NewStorageTierPolicy([]hatStorage.StorageTierRule{
		{Name: "hot", MinAge: 0, Placement: placement},
		{Name: "warm", MinAge: time.Hour, Placement: placement},
		{Name: "cold", MinAge: 24 * time.Hour, Placement: placement},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := range b.N {
		if _, err := policy.Select(time.Duration(index%48)*time.Hour, "part-001"); err != nil {
			b.Fatal(err)
		}
	}
}
