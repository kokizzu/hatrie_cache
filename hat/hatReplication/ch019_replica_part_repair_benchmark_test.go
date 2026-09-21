package hatReplication_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatMerkle"
	"hatrie_cache/hat/hatReplication"
)

var ch019PlanSink hatReplication.ReplicaPartRepairPlan

func BenchmarkCH019ReplicaPartRepairPlan(b *testing.B) {
	source, target := ch019BenchmarkInventories(b, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		plan, err := hatReplication.BuildReplicaPartRepairPlan(hatReplication.ReplicaPartRepairOptions{}, source, target)
		if err != nil {
			b.Fatal(err)
		}
		ch019PlanSink = plan
	}
}

func BenchmarkCH019ReplicaPartRepairMapBaseline(b *testing.B) {
	source, target := ch019BenchmarkInventories(b, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ch019PlanSink = ch019MapBaseline(source, target)
	}
}

func ch019BenchmarkInventories(b *testing.B, count int) (hatReplication.ReplicaPartInventory, hatReplication.ReplicaPartInventory) {
	b.Helper()
	source := hatReplication.ReplicaPartInventory{ReplicaID: "source", Generation: 10, Parts: make([]hatMerkle.PartCatalogEntry, count)}
	target := hatReplication.ReplicaPartInventory{ReplicaID: "target", Generation: 20, Parts: make([]hatMerkle.PartCatalogEntry, count)}
	for index := 0; index < count; index++ {
		name := fmt.Sprintf("part-%06d", index)
		manifest, err := hatMerkle.BuildPartManifest([]byte(name), nil)
		if err != nil {
			b.Fatal(err)
		}
		source.Parts[index] = hatMerkle.PartCatalogEntry{Name: name, Location: "source/" + name, Manifest: manifest}
		target.Parts[index] = hatMerkle.PartCatalogEntry{Name: name, Location: "target/" + name, Manifest: manifest}
	}
	return source, target
}

func ch019MapBaseline(source, target hatReplication.ReplicaPartInventory) hatReplication.ReplicaPartRepairPlan {
	sourceByName := make(map[string]hatMerkle.PartCatalogEntry, len(source.Parts))
	for _, part := range source.Parts {
		sourceByName[part.Name] = part
	}
	targetByName := make(map[string]hatMerkle.PartCatalogEntry, len(target.Parts))
	for _, part := range target.Parts {
		targetByName[part.Name] = part
	}
	plan := hatReplication.ReplicaPartRepairPlan{
		SourceReplica:    source.ReplicaID,
		TargetReplica:    target.ReplicaID,
		SourceGeneration: source.Generation,
		TargetGeneration: target.Generation,
	}
	for name, sourcePart := range sourceByName {
		targetPart, ok := targetByName[name]
		if !ok {
			plan.Actions = append(plan.Actions, hatReplication.ReplicaPartRepairAction{Kind: hatReplication.ReplicaPartRepairCopy, Name: name, Source: sourcePart})
			plan.SourceOnlyParts++
			continue
		}
		if sourcePart.Manifest.Equal(targetPart.Manifest) {
			plan.EqualParts++
			continue
		}
		plan.Actions = append(plan.Actions, hatReplication.ReplicaPartRepairAction{Kind: hatReplication.ReplicaPartRepairReplace, Name: name, Source: sourcePart, Target: targetPart})
		plan.ReplacedParts++
	}
	for name, targetPart := range targetByName {
		if _, ok := sourceByName[name]; ok {
			continue
		}
		plan.Actions = append(plan.Actions, hatReplication.ReplicaPartRepairAction{Kind: hatReplication.ReplicaPartRepairQuarantine, Name: name, Target: targetPart})
		plan.TargetOnlyParts++
	}
	return plan
}
