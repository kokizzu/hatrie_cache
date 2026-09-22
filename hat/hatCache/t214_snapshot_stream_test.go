//go:build t214

package hatCache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestT214StreamCommandJournalSnapshotWritesWithoutFilesystem(t *testing.T) {
	payload := []byte("streamed-snapshot-payload")
	digest := sha256.Sum256(payload)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Hatrie-Replication-Token"); got != "replica-secret" {
			t.Fatalf("replication token = %q, want replica-secret", got)
		}
		w.Header().Set("Content-Type", snapshotContentType)
		w.Header().Set("Content-Length", "25")
		w.Header().Set("X-Hatrie-Journal-Sequence", "42")
		w.Header().Set("X-Hatrie-Snapshot-Format", string(SnapshotFormatGzipBinary))
		w.Header().Set("X-Hatrie-Snapshot-SHA256", hex.EncodeToString(digest[:]))
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	var destination bytes.Buffer
	manifest, err := StreamCommandJournalSnapshot(context.Background(), server.URL, "replica-secret", server.Client(), &destination, 42)
	if err != nil {
		t.Fatalf("StreamCommandJournalSnapshot() error = %v", err)
	}
	if !bytes.Equal(destination.Bytes(), payload) {
		t.Fatalf("streamed bytes = %q, want %q", destination.Bytes(), payload)
	}
	want := SnapshotManifest{
		JournalSequence: 42,
		Format:          SnapshotFormatGzipBinary,
		SizeBytes:       int64(len(payload)),
		SHA256:          hex.EncodeToString(digest[:]),
	}
	if manifest != want {
		t.Fatalf("manifest = %#v, want %#v", manifest, want)
	}
}

func TestT214StreamCommandJournalSnapshotRejectsBadHeadersAndDigest(t *testing.T) {
	payload := []byte("streamed-snapshot-payload")
	digest := sha256.Sum256(payload)
	tests := []struct {
		name     string
		sequence string
		format   string
		checksum string
		minimum  uint64
		wantErr  error
	}{
		{name: "missing sequence", format: string(SnapshotFormatGzipBinary), checksum: hex.EncodeToString(digest[:]), wantErr: ErrSnapshotStreamMetadataInvalid},
		{name: "stale sequence", sequence: "41", format: string(SnapshotFormatGzipBinary), checksum: hex.EncodeToString(digest[:]), minimum: 42, wantErr: ErrSnapshotStreamSequenceTooOld},
		{name: "bad format", sequence: "42", format: "unknown", checksum: hex.EncodeToString(digest[:]), wantErr: ErrSnapshotStreamMetadataInvalid},
		{name: "bad checksum", sequence: "42", format: string(SnapshotFormatGzipBinary), checksum: hex.EncodeToString(make([]byte, sha256.Size)), wantErr: ErrSnapshotStreamDigestMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", snapshotContentType)
				if test.sequence != "" {
					w.Header().Set("X-Hatrie-Journal-Sequence", test.sequence)
				}
				w.Header().Set("X-Hatrie-Snapshot-Format", test.format)
				w.Header().Set("X-Hatrie-Snapshot-SHA256", test.checksum)
				w.Header().Set("Content-Length", "25")
				_, _ = w.Write(payload)
			}))
			defer server.Close()

			var destination bytes.Buffer
			_, err := StreamCommandJournalSnapshot(context.Background(), server.URL, "", server.Client(), &destination, test.minimum)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestT214StreamCommandJournalSnapshotPropagatesWriterError(t *testing.T) {
	payload := []byte("streamed-snapshot-payload")
	digest := sha256.Sum256(payload)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", snapshotContentType)
		w.Header().Set("X-Hatrie-Journal-Sequence", "42")
		w.Header().Set("X-Hatrie-Snapshot-Format", string(SnapshotFormatGzipBinary))
		w.Header().Set("X-Hatrie-Snapshot-SHA256", hex.EncodeToString(digest[:]))
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	_, err := StreamCommandJournalSnapshot(context.Background(), server.URL, "", server.Client(), failingSnapshotWriter{}, 42)
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("writer error = %v, want %v", err, io.ErrClosedPipe)
	}
}

type failingSnapshotWriter struct{}

func (failingSnapshotWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}
