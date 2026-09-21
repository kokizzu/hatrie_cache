package hatCache

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkC240BackupRestoreAndQuery(b *testing.B) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("jobs", `[{"id":1,"state":"queued"},{"id":2,"state":"running"}]`)
	bundlePath := filepath.Join(b.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		b.Fatalf("CreateBackupBundle() error = %v", err)
	}
	restoreRoot := b.TempDir()
	query := "FROM CACHE('jobs') AS job SELECT job.id, job.state"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		dataDir := filepath.Join(restoreRoot, "restore", fmt.Sprintf("%d", index))
		report, err := RestoreBackupBundle(bundlePath, dataDir, BackupBundleRestoreOptions{})
		if err != nil {
			b.Fatalf("RestoreBackupBundle() error = %v", err)
		}
		loaded := CreateHatTrie()
		if _, err := loaded.LoadSnapshotWithMetadata(report.Snapshot); err != nil {
			loaded.Destroy()
			b.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
		}
		rows := 0
		err = ExecuteSQLQueryRows(context.Background(), query, loaded, nil, SQLQueryOptions{}, func(_ []string, _ SQLRow) error {
			rows++
			return nil
		})
		loaded.Destroy()
		if err != nil {
			b.Fatalf("ExecuteSQLQueryRows() error = %v", err)
		}
		if rows != 2 {
			b.Fatalf("query rows = %d, want 2", rows)
		}
	}
}
