package hatBackup

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkCopyRestoreFiles(b *testing.B) {
	sourceDir := b.TempDir()
	files := make([]RestoreFile, 16)
	for index := range files {
		data := make([]byte, 64*1024+index*97)
		for offset := range data {
			data[offset] = byte(index + offset)
		}
		source := filepath.Join(sourceDir, "source", strconv.Itoa(index))
		if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
			b.Fatal(err)
		}
		if err := os.WriteFile(source, data, 0o600); err != nil {
			b.Fatal(err)
		}
		sum := sha256.Sum256(data)
		files[index] = RestoreFile{Source: source, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	}

	for _, variant := range []struct {
		name        string
		concurrency int
	}{
		{name: "serial", concurrency: 0},
		{name: "parallel-4", concurrency: 4},
	} {
		b.Run(variant.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(16 * 64 * 1024)
			for iteration := 0; iteration < b.N; iteration++ {
				destinationDir := filepath.Join(sourceDir, "dest", strconv.Itoa(iteration%4))
				b.StopTimer()
				if err := os.RemoveAll(destinationDir); err != nil {
					b.Fatal(err)
				}
				tasks := make([]RestoreFile, len(files))
				for index, file := range files {
					tasks[index] = file
					tasks[index].Destination = filepath.Join(destinationDir, strconv.Itoa(index), "payload")
				}
				b.StartTimer()
				if err := CopyRestoreFiles(tasks, RestoreFileOptions{MaxConcurrency: variant.concurrency}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
