package hatBackup_test

import (
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatBackup"
)

func TestRestoreReportUsesPortableBackupModels(t *testing.T) {
	report := hatBackup.RestoreReport{
		OK:                  true,
		Bundle:              "nightly.tar.gz",
		DataDir:             "/var/lib/hatrie",
		Mode:                hatBackup.ModePebbleCheckpoint,
		Partition:           &hatBackup.PartitionMetadata{Mode: "partitioned", Partitions: []string{"sg"}},
		PartitionValidation: &hatBackup.PartitionValidation{OK: true, CheckedKeys: 4},
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var decoded hatBackup.RestoreReport
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if !decoded.OK || decoded.Mode != hatBackup.ModePebbleCheckpoint || decoded.PartitionValidation == nil || decoded.PartitionValidation.CheckedKeys != 4 {
		t.Fatalf("decoded report = %#v", decoded)
	}
}
