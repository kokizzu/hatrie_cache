package hatCache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"

	"hatrie_cache/hat/hatSnapshot"
)

// SnapshotManifest is the exact byte-level identity of one streamed snapshot.
// It includes the journal coordinate needed to continue replay after restore.
type SnapshotManifest = hatSnapshot.Manifest

var ErrSnapshotManifestMismatch = errors.New("hatriecache: snapshot manifest mismatch")

// WriteSnapshotWithManifest writes an online snapshot and returns its exact
// journal coordinate, canonical format, byte count, and SHA-256 digest. The
// existing WriteSnapshotWithFormat path remains allocation and CPU compatible
// for callers that do not need a transfer manifest.
func (journal *CommandJournal) WriteSnapshotWithManifest(trie *HatTrie, writer io.Writer, format SnapshotFormat) (SnapshotManifest, error) {
	if journal == nil {
		return SnapshotManifest{}, ErrNilCommandJournal
	}
	if trie == nil {
		return SnapshotManifest{}, ErrNilHatTrie
	}
	if writer == nil {
		return SnapshotManifest{}, errors.New("hatriecache: snapshot writer is nil")
	}
	format, err := ParseSnapshotFormat(string(format))
	if err != nil {
		return SnapshotManifest{}, err
	}

	manifestWriter := snapshotManifestWriter{writer: writer, digest: sha256.New()}
	metadata, err := journal.WriteSnapshotWithFormat(trie, &manifestWriter, format)
	if err != nil {
		return SnapshotManifest{}, err
	}
	return SnapshotManifest{
		JournalSequence: metadata.JournalSequence,
		Format:          format,
		SizeBytes:       manifestWriter.size,
		SHA256:          hex.EncodeToString(manifestWriter.digest.Sum(nil)),
	}, nil
}

type snapshotManifestWriter struct {
	writer io.Writer
	digest hash.Hash
	size   int64
}

func (writer *snapshotManifestWriter) Write(data []byte) (int, error) {
	n, err := writer.writer.Write(data)
	if n > 0 {
		_, _ = writer.digest.Write(data[:n])
		writer.size += int64(n)
	}
	return n, err
}

// VerifySnapshotManifest verifies snapshot bytes and the embedded journal
// coordinate against a manifest produced by WriteSnapshotWithManifest.
func VerifySnapshotManifest(path string, expected SnapshotManifest) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	digest := sha256.New()
	size, copyErr := io.Copy(digest, file)
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if size != expected.SizeBytes {
		return fmt.Errorf("%w: size=%d want=%d", ErrSnapshotManifestMismatch, size, expected.SizeBytes)
	}
	if got := hex.EncodeToString(digest.Sum(nil)); got != expected.SHA256 {
		return fmt.Errorf("%w: sha256=%s want=%s", ErrSnapshotManifestMismatch, got, expected.SHA256)
	}
	metadata, err := ReadSnapshotMetadata(path)
	if err != nil {
		return err
	}
	if metadata.JournalSequence != expected.JournalSequence {
		return fmt.Errorf("%w: journal sequence=%d want=%d", ErrSnapshotManifestMismatch, metadata.JournalSequence, expected.JournalSequence)
	}
	return nil
}
