package hatBackup_test

import (
	"testing"

	"hatrie_cache/hat/hatBackup"
)

func TestBackupModelParsesAndNormalizesPartitionMetadata(t *testing.T) {
	mode, err := hatBackup.ParseMode(" pebble-checkpoint ")
	if err != nil || mode != hatBackup.ModePebbleCheckpoint {
		t.Fatalf("ParseMode() = %q, %v", mode, err)
	}

	partition, err := hatBackup.NormalizePartitionMetadata(hatBackup.PartitionMetadata{
		NodeID:      " node-a ",
		Partitions:  []string{" east-2 ", "east-1"},
		KeyPrefixes: []string{" customer: ", "session:"},
	})
	if err != nil {
		t.Fatalf("NormalizePartitionMetadata() error = %v", err)
	}
	if partition.Mode != "partitioned" || partition.NodeID != "node-a" {
		t.Fatalf("normalized partition = %+v", partition)
	}
	if partition.Partitions[0] != "east-2" || partition.KeyPrefixes[0] != "customer:" {
		t.Fatalf("partition strings were not trimmed: %+v", partition)
	}

	clone := hatBackup.ClonePartitionMetadata(partition)
	clone.Partitions[0] = "changed"
	if partition.Partitions[0] == "changed" {
		t.Fatal("ClonePartitionMetadata() retained mutable backing")
	}
}

func TestBackupModelRejectsInvalidPartitionMetadata(t *testing.T) {
	if _, err := hatBackup.ParseMode("invalid"); err == nil {
		t.Fatal("ParseMode() error = nil, want rejection")
	}
	if _, err := hatBackup.NormalizePartitionMetadata(hatBackup.PartitionMetadata{NodeID: "node-a"}); err == nil {
		t.Fatal("NormalizePartitionMetadata() error = nil, want rejection")
	}
}
