//go:build t214baseline

package hatCache

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func BenchmarkT214SnapshotPullBaseline(b *testing.B) {
	payload, digest := t214BenchmarkSnapshot(b)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", snapshotContentType)
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
		w.Header().Set("X-Hatrie-Journal-Sequence", "128")
		w.Header().Set("X-Hatrie-Snapshot-Format", string(SnapshotFormatGzipBinary))
		w.Header().Set("X-Hatrie-Snapshot-SHA256", hex.EncodeToString(digest[:]))
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	path := filepath.Join(b.TempDir(), "pulled.snapshot")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := PullCommandJournalSnapshot(context.Background(), server.URL, "", server.Client(), path); err != nil {
			b.Fatal(err)
		}
	}
}
