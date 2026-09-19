package hatBackup_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatBackup"
)

func TestM050LogicalSnapshotPublicContract(t *testing.T) {
	manifest := hatBackup.LogicalSnapshotManifest{
		Version:           hatBackup.LogicalSnapshotManifestVersion,
		SnapshotID:        "public-snapshot",
		CreatedAt:         time.Unix(1_700_000_001, 0).UTC(),
		BundleBackupID:    "backup-1",
		StorageGeneration: 1,
		JournalSequence:   2,
		Frontiers:         []hatBackup.FrontierCheckpoint{{ID: "orders", Lower: 2, Upper: 2}},
		Subscriptions:     []hatBackup.SubscriptionCheckpoint{{ID: "orders-sub", FrontierID: "orders", AsOf: 2, AckedSequence: 2}},
	}
	payload, err := manifest.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	decoded, err := hatBackup.DecodeLogicalSnapshotManifest(payload)
	if err != nil {
		t.Fatalf("DecodeLogicalSnapshotManifest() error = %v", err)
	}
	if decoded.SnapshotID != manifest.SnapshotID || decoded.JournalSequence != manifest.JournalSequence {
		t.Fatalf("decoded manifest = %#v", decoded)
	}
}
