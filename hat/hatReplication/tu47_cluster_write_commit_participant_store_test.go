package hatReplication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestClusterWriteCommitParticipantFileStoreRoundTripsAndUsesPrivateFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "participant.state")
	store, err := NewClusterWriteCommitParticipantFileStore(ClusterWriteCommitParticipantFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	participant, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	proposal := ClusterWriteCommitProposal{
		TransactionID: "tx-round-trip",
		Sequence:      7,
		FenceToken:    11,
		PayloadDigest: sha256.Sum256([]byte("payload")),
	}
	if _, err := participant.Prepare(proposal); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), participant); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("participant state mode = %o, want 600", got)
	}
	restored, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	found, err := store.Load(context.Background(), restored)
	if err != nil || !found {
		t.Fatalf("Load() = %t/%v, want true/nil", found, err)
	}
	got, ok := restored.Status(proposal.TransactionID)
	if !ok || got.Proposal != proposal || got.Phase != ClusterWriteCommitParticipantPrepared {
		t.Fatalf("restored status = %#v/%t, want prepared proposal", got, ok)
	}
	before, err := participant.MarshalSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	after, err := restored.MarshalSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("restored snapshot differs: %x != %x", before, after)
	}
}

func TestClusterWriteCommitParticipantFileStoreRejectsCorruptionAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "participant.state")
	store, err := NewClusterWriteCommitParticipantFileStore(ClusterWriteCommitParticipantFileStoreOptions{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	proposal := ClusterWriteCommitProposal{TransactionID: "tx-source", Sequence: 3, FenceToken: 5}
	if _, err := source.Prepare(proposal); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	target, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	existing := ClusterWriteCommitProposal{TransactionID: "tx-existing", Sequence: 1, FenceToken: 2}
	if _, err := target.Prepare(existing); err != nil {
		t.Fatal(err)
	}
	if found, err := store.Load(context.Background(), target); found || !errors.Is(err, ErrClusterWriteCommitParticipantFileStoreSnapshotInvalid) {
		t.Fatalf("Load(corrupt) = %t/%v, want false/snapshot-invalid", found, err)
	}
	if _, ok := target.Status(existing.TransactionID); !ok {
		t.Fatal("corrupt load changed existing participant state")
	}
	if _, ok := target.Status(proposal.TransactionID); ok {
		t.Fatal("corrupt load partially restored source state")
	}
}

func TestClusterWriteCommitParticipantFileStoreValidatesOptionsAndContext(t *testing.T) {
	if _, err := NewClusterWriteCommitParticipantFileStore(ClusterWriteCommitParticipantFileStoreOptions{}); !errors.Is(err, ErrClusterWriteCommitParticipantFileStorePathEmpty) {
		t.Fatalf("empty path error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "participant.state")
	store, err := NewClusterWriteCommitParticipantFileStore(ClusterWriteCommitParticipantFileStoreOptions{Path: path, MaxBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	participant, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(nil, participant); !errors.Is(err, ErrClusterWriteCommitParticipantFileStoreContextInvalid) {
		t.Fatalf("nil Save context error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Save(ctx, participant); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Save context error = %v", err)
	}
}

func BenchmarkTU047ParticipantFileStore(b *testing.B) {
	participant, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 8})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := participant.Prepare(ClusterWriteCommitProposal{TransactionID: "tx-benchmark", Sequence: 1, FenceToken: 2, PayloadDigest: sha256.Sum256([]byte("benchmark"))}); err != nil {
		b.Fatal(err)
	}
	snapshot, err := participant.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	store, err := NewClusterWriteCommitParticipantFileStore(ClusterWriteCommitParticipantFileStoreOptions{Path: filepath.Join(b.TempDir(), "participant.state")})
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(snapshot)))
	b.Run("marshal", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			if _, err := participant.MarshalSnapshot(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("save", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			if err := store.Save(context.Background(), participant); err != nil {
				b.Fatal(err)
			}
		}
	})
	restored, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 8})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("load", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			if found, err := store.Load(context.Background(), restored); err != nil || !found {
				b.Fatalf("Load() = %t/%v", found, err)
			}
		}
	})
}
