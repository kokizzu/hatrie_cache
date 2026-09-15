package hatBackup_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatBackup"
)

func BenchmarkMZ006RetentionPlanBaseline(b *testing.B) {
	manifest := mz006Manifest("base", "", false, fmt.Sprintf("%064x", 1))
	manifests := []hatBackup.BundleManifest{manifest}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := hatBackup.PlanBackupRetention(manifests, "base", 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ006GarbageCollectionPlanAfter(b *testing.B) {
	store := newMZ006ObjectStore()
	keepHash := fmt.Sprintf("%064x", 1)
	store.put("backup/objects/"+keepHash, nil)
	for index := 2; index <= 513; index++ {
		store.put("backup/objects/"+fmt.Sprintf("%064x", index), nil)
	}
	target, err := hatBackup.NewObjectStoreTarget(store, "backup")
	if err != nil {
		b.Fatal(err)
	}
	retention, err := hatBackup.PlanBackupRetention([]hatBackup.BundleManifest{
		mz006Manifest("base", "", false, keepHash),
	}, "base", 1)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := target.PlanGarbageCollection(context.Background(), retention); err != nil {
			b.Fatal(err)
		}
	}
}
