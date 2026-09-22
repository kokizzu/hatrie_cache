//go:build t214

package hatCache

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func BenchmarkT214SnapshotStream(b *testing.B) {
	payload, digest := t214BenchmarkSnapshot(b)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", snapshotContentType)
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.Header().Set("X-Hatrie-Journal-Sequence", "128")
		w.Header().Set("X-Hatrie-Snapshot-Format", string(SnapshotFormatGzipBinary))
		w.Header().Set("X-Hatrie-Snapshot-SHA256", hex.EncodeToString(digest[:]))
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := StreamCommandJournalSnapshot(context.Background(), server.URL, "", server.Client(), io.Discard, 128); err != nil {
			b.Fatal(err)
		}
	}
}
