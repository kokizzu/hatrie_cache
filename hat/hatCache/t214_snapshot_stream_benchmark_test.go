package hatCache

import (
	"io"
	"path/filepath"
	"testing"
)

func BenchmarkT214SnapshotTransferModes(b *testing.B) {
	for _, mode := range []string{"FilesystemRoundTrip", "StreamRoundTrip"} {
		b.Run(mode, func(b *testing.B) {
			source := CreateHatTrie()
			defer source.Destroy()
			source.UpsertString("benchmark", "snapshot-transfer")
			target := CreateHatTrie()
			defer target.Destroy()
			path := filepath.Join(b.TempDir(), "snapshot.hc")

			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if mode == "FilesystemRoundTrip" {
					if err := source.SaveSnapshotWithFormat(path, SnapshotFormatBinary); err != nil {
						b.Fatalf("SaveSnapshotWithFormat() error = %v", err)
					}
					if err := target.LoadSnapshot(path); err != nil {
						b.Fatalf("LoadSnapshot() error = %v", err)
					}
					continue
				}

				reader, writer := io.Pipe()
				writeErr := make(chan error, 1)
				go func() {
					err := source.WriteSnapshotToWithJournalSequenceAndFormat(writer, 73, SnapshotFormatBinary)
					_ = writer.CloseWithError(err)
					writeErr <- err
				}()
				if _, err := target.LoadSnapshotFromWithMetadata(reader); err != nil {
					b.Fatalf("LoadSnapshotFromWithMetadata() error = %v", err)
				}
				if err := <-writeErr; err != nil {
					b.Fatalf("WriteSnapshotToWithJournalSequenceAndFormat() error = %v", err)
				}
			}
		})
	}
}
