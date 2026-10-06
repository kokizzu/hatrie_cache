package hatBackup

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"
	"time"
)

func BenchmarkTG20BaselineExistingChainPlan(b *testing.B) {
	manifests := tg20BaselineBenchmarkManifests()
	latestID := manifests[len(manifests)-1].BackupID
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		plan, err := PlanBackupChain(manifests, latestID)
		if err != nil {
			b.Fatal(err)
		}
		if plan.LatestBackupID != latestID {
			b.Fatal("baseline chain plan selected the wrong tip")
		}
	}
}

func tg20BaselineManifest(id, parent string, created time.Time, sequence uint64, bytes int64) BundleManifest {
	digest := sha256.Sum256([]byte(id))
	return BundleManifest{
		Version:           BundleVersion,
		CreatedAt:         created,
		Mode:              ModePebbleIncremental,
		Snapshot:          id,
		BackupID:          id,
		ParentBackupID:    parent,
		Incremental:       parent != "",
		Store:             "cache",
		NewObjectBytes:    bytes,
		StorageBackend:    "pebble",
		StorageFormat:     "v1",
		StorageIdentity:   "cache",
		StorageGeneration: 1,
		JournalSequence:   sequence,
		Files:             []BundleFile{{Path: id + ".snapshot", Size: bytes, SHA256: hex.EncodeToString(digest[:])}},
	}
}

func tg20BaselineBenchmarkManifests() []BundleManifest {
	baseTime := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	manifests := make([]BundleManifest, 0, 64)
	parent := ""
	for index := 0; index < 64; index++ {
		id := "backup-" + strconv.Itoa(index)
		manifests = append(manifests, tg20BaselineManifest(id, parent, baseTime.Add(time.Duration(index)*time.Hour), uint64(index+1), 1024))
		parent = id
	}
	return manifests
}
