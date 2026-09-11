package hatCache

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkRestoreBundleResumeExtraction(b *testing.B) {
	source := CreateHatTrie()
	defer source.Destroy()
	value := strings.Repeat("resume-payload-", 16)
	for i := 0; i < 2048; i++ {
		source.UpsertString("resume:benchmark:"+strconv.Itoa(i), value)
	}
	bundlePath := filepath.Join(b.TempDir(), "resume-benchmark.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:           BackupModeSnapshot,
		SnapshotFormat: SnapshotFormatBinary,
	}); err != nil {
		b.Fatal(err)
	}
	manifest, err := readBackupBundleManifest(bundlePath)
	if err != nil {
		b.Fatal(err)
	}
	archiveInfo, err := os.Stat(bundlePath)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("fresh", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(archiveInfo.Size())
		root := filepath.Join(b.TempDir(), "fresh")
		for i := 0; i < b.N; i++ {
			destination := filepath.Join(root, strconv.Itoa(i))
			if err := extractBackupBundleFiles(bundlePath, destination, manifest.Files); err != nil {
				b.Fatal(err)
			}
		}
	})

	resumeDestination := filepath.Join(b.TempDir(), "resume")
	if err := extractBackupBundleFiles(bundlePath, resumeDestination, manifest.Files); err != nil {
		b.Fatal(err)
	}
	b.Run("resume", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(archiveInfo.Size())
		for i := 0; i < b.N; i++ {
			if err := extractBackupBundleFilesWithResume(bundlePath, resumeDestination, manifest.Files); err != nil {
				b.Fatal(err)
			}
		}
	})
}
