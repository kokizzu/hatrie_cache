package hatReplication_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatMerkle"
	"hatrie_cache/hat/hatReplication"
)

func TestCH019ReplicaPartRepairPlanIsDeterministicAndNonMutating(t *testing.T) {
	source := hatReplication.ReplicaPartInventory{
		ReplicaID:  " source-a ",
		Generation: 7,
		Parts: []hatMerkle.PartCatalogEntry{
			ch019Part(t, "part-b", "source-b", "bravo"),
			ch019Part(t, "part-a", "source-a", "alpha"),
		},
	}
	target := hatReplication.ReplicaPartInventory{
		ReplicaID:  "target-a",
		Generation: 9,
		Parts: []hatMerkle.PartCatalogEntry{
			ch019Part(t, "part-c", "target-c", "charlie"),
			ch019Part(t, "part-a", "target-a", "alpha"),
		},
	}

	plan, err := hatReplication.BuildReplicaPartRepairPlan(hatReplication.ReplicaPartRepairOptions{}, source, target)
	if err != nil {
		t.Fatalf("BuildReplicaPartRepairPlan() error = %v", err)
	}
	if plan.SourceReplica != "source-a" || plan.TargetReplica != "target-a" {
		t.Fatalf("replica IDs = %q -> %q", plan.SourceReplica, plan.TargetReplica)
	}
	if plan.SourceGeneration != 7 || plan.TargetGeneration != 9 {
		t.Fatalf("generations = %d -> %d", plan.SourceGeneration, plan.TargetGeneration)
	}
	if plan.EqualParts != 1 || plan.SourceOnlyParts != 1 || plan.TargetOnlyParts != 1 || plan.ReplacedParts != 0 {
		t.Fatalf("plan counts = %+v", plan)
	}
	if len(plan.Actions) != 2 {
		t.Fatalf("action count = %d, want 2", len(plan.Actions))
	}
	if plan.Actions[0].Kind != hatReplication.ReplicaPartRepairCopy || plan.Actions[0].Name != "part-b" {
		t.Fatalf("first action = %+v", plan.Actions[0])
	}
	if plan.Actions[1].Kind != hatReplication.ReplicaPartRepairQuarantine || plan.Actions[1].Name != "part-c" {
		t.Fatalf("second action = %+v", plan.Actions[1])
	}
	if source.Parts[0].Name != "part-b" || source.Parts[1].Name != "part-a" {
		t.Fatalf("source inventory was reordered: %+v", source.Parts)
	}
	if !plan.IsCurrent(7, 9) || plan.IsCurrent(8, 9) {
		t.Fatalf("plan generation fence is incorrect: %+v", plan)
	}
}

func TestCH019ReplicaPartRepairPlanReplacesMismatchedPart(t *testing.T) {
	source := hatReplication.ReplicaPartInventory{
		ReplicaID: "source",
		Parts:     []hatMerkle.PartCatalogEntry{ch019Part(t, "same", "source", "new")},
	}
	target := hatReplication.ReplicaPartInventory{
		ReplicaID: "target",
		Parts:     []hatMerkle.PartCatalogEntry{ch019Part(t, "same", "target", "old")},
	}

	plan, err := hatReplication.BuildReplicaPartRepairPlan(hatReplication.ReplicaPartRepairOptions{}, source, target)
	if err != nil {
		t.Fatalf("BuildReplicaPartRepairPlan() error = %v", err)
	}
	if plan.ReplacedParts != 1 || len(plan.Actions) != 1 {
		t.Fatalf("plan = %+v", plan)
	}
	action := plan.Actions[0]
	if action.Kind != hatReplication.ReplicaPartRepairReplace || action.Source.Location != "source" || action.Target.Location != "target" {
		t.Fatalf("replacement action = %+v", action)
	}
	if action.Source.Manifest.Equal(action.Target.Manifest) {
		t.Fatal("replacement action has equal manifests")
	}
}

func TestCH019ReplicaPartRepairPlanValidatesBoundsAndDuplicates(t *testing.T) {
	valid := ch019Part(t, "one", "source", "one")
	cases := []struct {
		name   string
		option hatReplication.ReplicaPartRepairOptions
		source hatReplication.ReplicaPartInventory
		want   error
	}{
		{
			name:   "same replica",
			source: hatReplication.ReplicaPartInventory{ReplicaID: "same", Parts: []hatMerkle.PartCatalogEntry{valid}},
			want:   hatReplication.ErrReplicaPartRepairInvalid,
		},
		{
			name: "duplicate source part",
			source: hatReplication.ReplicaPartInventory{
				ReplicaID: "source",
				Parts:     []hatMerkle.PartCatalogEntry{valid, valid},
			},
			want: hatReplication.ErrReplicaPartRepairInvalid,
		},
		{
			name:   "zero max parts",
			option: hatReplication.ReplicaPartRepairOptions{MaxParts: -1},
			want:   hatReplication.ErrReplicaPartRepairOptionsInvalid,
		},
		{
			name:   "capacity",
			option: hatReplication.ReplicaPartRepairOptions{MaxParts: 1},
			source: hatReplication.ReplicaPartInventory{ReplicaID: "source", Parts: []hatMerkle.PartCatalogEntry{valid, ch019Part(t, "two", "source", "two")}},
			want:   hatReplication.ErrReplicaPartRepairCapacity,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			target := hatReplication.ReplicaPartInventory{ReplicaID: "target"}
			if test.name == "same replica" {
				target.ReplicaID = "same"
			}
			_, err := hatReplication.BuildReplicaPartRepairPlan(test.option, test.source, target)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func ch019Part(t *testing.T, name, location, value string) hatMerkle.PartCatalogEntry {
	t.Helper()
	data := []byte(value)
	manifest, err := hatMerkle.BuildPartManifest(data, []hatMerkle.PartColumnRange{{Name: "value", Size: uint64(len(data))}})
	if err != nil {
		t.Fatalf("BuildPartManifest() error = %v", err)
	}
	return hatMerkle.PartCatalogEntry{Name: name, Location: location, Manifest: manifest}
}
