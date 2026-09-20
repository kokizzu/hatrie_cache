package hatBackup

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func snapshotJoinTestManifest() SnapshotJoinManifest {
	var checksum [32]byte
	checksum[0] = 7
	return SnapshotJoinManifest{
		ProtocolVersion:  SnapshotJoinProtocolVersion,
		SourceID:         "primary-a",
		JoinerID:         "replica-b",
		FencingToken:     42,
		SnapshotSequence: 10,
		WALLastSequence:  12,
		SnapshotChecksum: checksum,
	}
}

func TestSnapshotJoinBootstrapRequiresContiguousWALAndFencedActivation(t *testing.T) {
	manifest := snapshotJoinTestManifest()
	bootstrap, err := NewSnapshotJoinBootstrap(manifest, SnapshotJoinOptions{MaxBatchRecords: 2})
	if err != nil {
		t.Fatalf("NewSnapshotJoinBootstrap() error = %v", err)
	}

	called := false
	if err := bootstrap.ApplySnapshot(context.Background(), manifest.SnapshotChecksum, func() error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("ApplySnapshot() error = %v", err)
	}
	if !called {
		t.Fatal("ApplySnapshot() did not invoke the snapshot callback")
	}

	records := []SnapshotJoinRecord{{Sequence: 11, Payload: []byte("first")}}
	var callbackRecords []SnapshotJoinRecord
	if err := bootstrap.ApplyWAL(context.Background(), records, func(batch []SnapshotJoinRecord) error {
		callbackRecords = append([]SnapshotJoinRecord(nil), batch...)
		callbackRecords[0].Payload = append([]byte(nil), batch[0].Payload...)
		return nil
	}); err != nil {
		t.Fatalf("ApplyWAL(first) error = %v", err)
	}
	records[0].Payload[0] = 'X'
	if string(callbackRecords[0].Payload) != "first" {
		t.Fatalf("callback payload changed with caller buffer: %q", callbackRecords[0].Payload)
	}

	if err := bootstrap.ApplyWAL(context.Background(), []SnapshotJoinRecord{{Sequence: 13, Payload: []byte("gap")}}, func([]SnapshotJoinRecord) error {
		return nil
	}); !errors.Is(err, ErrSnapshotJoinSequenceGap) {
		t.Fatalf("ApplyWAL(gap) error = %v, want ErrSnapshotJoinSequenceGap", err)
	}
	status := bootstrap.Status()
	if status.AppliedSequence != 11 || status.Phase != SnapshotJoinPhaseSnapshotApplied {
		t.Fatalf("status after gap = %#v, want sequence 11 and snapshot-applied phase", status)
	}

	if err := bootstrap.ApplyWAL(context.Background(), []SnapshotJoinRecord{{Sequence: 12, Payload: []byte("last")}}, func([]SnapshotJoinRecord) error {
		return nil
	}); err != nil {
		t.Fatalf("ApplyWAL(last) error = %v", err)
	}
	if err := bootstrap.Activate(context.Background(), manifest.FencingToken+1, func() error { return nil }); !errors.Is(err, ErrSnapshotJoinFence) {
		t.Fatalf("Activate(stale fence) error = %v, want ErrSnapshotJoinFence", err)
	}

	activated := false
	if err := bootstrap.Activate(context.Background(), manifest.FencingToken, func() error {
		activated = true
		return nil
	}); err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	if !activated || bootstrap.Status().Phase != SnapshotJoinPhaseActive {
		t.Fatalf("activation state = %#v, activated=%v", bootstrap.Status(), activated)
	}
}

func TestSnapshotJoinCheckpointRoundTripResumesWithoutLosingProgress(t *testing.T) {
	manifest := snapshotJoinTestManifest()
	first, err := NewSnapshotJoinBootstrap(manifest, SnapshotJoinOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ApplySnapshot(context.Background(), manifest.SnapshotChecksum, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := first.ApplyWAL(context.Background(), []SnapshotJoinRecord{{Sequence: 11, Payload: []byte("one")}}, func([]SnapshotJoinRecord) error { return nil }); err != nil {
		t.Fatal(err)
	}

	checkpoint := first.Checkpoint()
	encoded, err := checkpoint.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	decoded, err := UnmarshalSnapshotJoinCheckpoint(encoded)
	if err != nil {
		t.Fatalf("UnmarshalSnapshotJoinCheckpoint() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, checkpoint) {
		t.Fatalf("decoded checkpoint = %#v, want %#v", decoded, checkpoint)
	}

	resumed, err := NewSnapshotJoinBootstrap(manifest, SnapshotJoinOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.RestoreCheckpoint(decoded); err != nil {
		t.Fatalf("RestoreCheckpoint() error = %v", err)
	}
	if err := resumed.ApplyWAL(context.Background(), []SnapshotJoinRecord{{Sequence: 12, Payload: []byte("two")}}, func([]SnapshotJoinRecord) error { return nil }); err != nil {
		t.Fatalf("resumed ApplyWAL() error = %v", err)
	}
	if err := resumed.Activate(context.Background(), manifest.FencingToken, func() error { return nil }); err != nil {
		t.Fatalf("resumed Activate() error = %v", err)
	}
	if status := resumed.Status(); status.Phase != SnapshotJoinPhaseActive || status.AppliedSequence != 12 {
		t.Fatalf("resumed status = %#v", status)
	}

	encoded[len(encoded)-1] ^= 1
	if _, err := UnmarshalSnapshotJoinCheckpoint(encoded); !errors.Is(err, ErrSnapshotJoinCheckpointCorrupt) {
		t.Fatalf("corrupt checkpoint error = %v, want ErrSnapshotJoinCheckpointCorrupt", err)
	}
}

func TestSnapshotJoinBootstrapFailureIsTerminalAndBoundsInput(t *testing.T) {
	manifest := snapshotJoinTestManifest()
	bootstrap, err := NewSnapshotJoinBootstrap(manifest, SnapshotJoinOptions{MaxBatchRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.ApplySnapshot(context.Background(), manifest.SnapshotChecksum, func() error { return errors.New("snapshot failed") }); err == nil {
		t.Fatal("ApplySnapshot() error = nil, want callback failure")
	}
	if bootstrap.Status().Phase != SnapshotJoinPhaseFailed {
		t.Fatalf("failed phase = %v, want failed", bootstrap.Status().Phase)
	}
	if _, err := bootstrap.Checkpoint().MarshalBinary(); err != nil {
		t.Fatalf("failed checkpoint MarshalBinary() error = %v", err)
	}
	if err := bootstrap.ApplyWAL(context.Background(), []SnapshotJoinRecord{{Sequence: 11}}, func([]SnapshotJoinRecord) error { return nil }); !errors.Is(err, ErrSnapshotJoinFailed) {
		t.Fatalf("ApplyWAL() after failure = %v, want ErrSnapshotJoinFailed", err)
	}

	if _, err := NewSnapshotJoinBootstrap(manifest, SnapshotJoinOptions{MaxBatchRecords: -1}); !errors.Is(err, ErrSnapshotJoinOptionsInvalid) {
		t.Fatalf("invalid options error = %v, want ErrSnapshotJoinOptionsInvalid", err)
	}
	if err := bootstrap.RestoreCheckpoint(SnapshotJoinCheckpoint{Manifest: manifest}); !errors.Is(err, ErrSnapshotJoinFailed) {
		t.Fatalf("RestoreCheckpoint() after failure = %v, want ErrSnapshotJoinFailed", err)
	}
}

func TestSnapshotJoinStatusCanBeReadWhileCallbacksRun(t *testing.T) {
	manifest := snapshotJoinTestManifest()
	bootstrap, err := NewSnapshotJoinBootstrap(manifest, SnapshotJoinOptions{MaxBatchRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- bootstrap.ApplySnapshot(context.Background(), manifest.SnapshotChecksum, func() error {
			close(started)
			<-release
			return nil
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("snapshot callback did not start")
	}
	if status := bootstrap.Status(); status.Phase != SnapshotJoinPhaseSnapshotApplying {
		t.Fatalf("status during snapshot callback = %#v, want snapshot-applying", status)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	walStarted := make(chan struct{})
	walRelease := make(chan struct{})
	go func() {
		done <- bootstrap.ApplyWAL(context.Background(), []SnapshotJoinRecord{{Sequence: 11, Payload: []byte("one")}}, func([]SnapshotJoinRecord) error {
			close(walStarted)
			<-walRelease
			return nil
		})
	}()
	select {
	case <-walStarted:
	case <-time.After(time.Second):
		t.Fatal("WAL callback did not start")
	}
	if status := bootstrap.Status(); status.Phase != SnapshotJoinPhaseWALApplying {
		t.Fatalf("status during WAL callback = %#v, want WAL-applying", status)
	}
	close(walRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
