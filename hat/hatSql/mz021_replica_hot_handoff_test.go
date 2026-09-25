package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type mz021ReplicaHandoffTestSource struct {
	snapshot ReplicaHotHandoffSnapshot
	batches  map[uint64]ReplicaHotHandoffBatch
	fetches  int
	limit    int
}

func (source *mz021ReplicaHandoffTestSource) Snapshot(context.Context) (ReplicaHotHandoffSnapshot, error) {
	snapshot := source.snapshot
	snapshot.Payload = append([]byte(nil), snapshot.Payload...)
	return snapshot, nil
}

func (source *mz021ReplicaHandoffTestSource) Fetch(_ context.Context, after uint64, limit int) (ReplicaHotHandoffBatch, error) {
	source.fetches++
	source.limit = limit
	batch, ok := source.batches[after]
	if !ok {
		return ReplicaHotHandoffBatch{Epoch: source.snapshot.Epoch, SourceFrontier: after}, nil
	}
	return batch, nil
}

type mz021ReplicaHandoffTestTarget struct {
	installed  ReplicaHotHandoffSnapshot
	apply      []ReplicaHotHandoffDelta
	ready      int
	installErr error
	applyErr   error
	readyErr   error
}

func (target *mz021ReplicaHandoffTestTarget) InstallSnapshot(_ context.Context, snapshot ReplicaHotHandoffSnapshot) error {
	if target.installErr != nil {
		return target.installErr
	}
	target.installed = snapshot
	return nil
}

func (target *mz021ReplicaHandoffTestTarget) Apply(_ context.Context, batch ReplicaHotHandoffBatch) error {
	if target.applyErr != nil {
		return target.applyErr
	}
	target.apply = append(target.apply, batch.Deltas...)
	return nil
}

func (target *mz021ReplicaHandoffTestTarget) Ready(context.Context) error {
	if target.readyErr != nil {
		return target.readyErr
	}
	target.ready++
	return nil
}

func newMZ021ReplicaHotHandoffTest(t *testing.T, source *mz021ReplicaHandoffTestSource, target *mz021ReplicaHandoffTestTarget) *ReplicaHotHandoff {
	t.Helper()
	handoff, err := NewReplicaHotHandoff(ReplicaHotHandoffOptions{
		SourceID:     "primary",
		TargetID:     "replica-b",
		BatchSize:    2,
		PollInterval: time.Nanosecond,
		FencingToken: 9,
	})
	if err != nil {
		t.Fatalf("NewReplicaHotHandoff() error = %v", err)
	}
	if err := handoff.Prepare(context.Background(), source, target); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	return handoff
}

func TestMZ021ReplicaHotHandoffPreparesCatchesUpAndPromotes(t *testing.T) {
	source := &mz021ReplicaHandoffTestSource{
		snapshot: ReplicaHotHandoffSnapshot{
			SourceID:      "primary",
			SnapshotID:    "snapshot-1",
			Epoch:         4,
			SchemaVersion: 7,
			Frontier:      2,
			Payload:       []byte("warm-query-state"),
		},
		batches: map[uint64]ReplicaHotHandoffBatch{
			2: {
				Epoch:          4,
				SourceFrontier: 4,
				Deltas: []ReplicaHotHandoffDelta{
					{Sequence: 3, Payload: []byte("delta-3")},
					{Sequence: 4, Payload: []byte("delta-4")},
				},
			},
		},
	}
	target := &mz021ReplicaHandoffTestTarget{}
	handoff, err := NewReplicaHotHandoff(ReplicaHotHandoffOptions{
		SourceID:              "primary",
		TargetID:              "replica-b",
		ExpectedSchemaVersion: 7,
		BatchSize:             2,
		PollInterval:          time.Nanosecond,
		FencingToken:          9,
	})
	if err != nil {
		t.Fatalf("NewReplicaHotHandoff() error = %v", err)
	}
	if err := handoff.Prepare(context.Background(), source, target); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if err := handoff.CatchUp(context.Background(), source, target); err != nil {
		t.Fatalf("CatchUp() error = %v", err)
	}
	state := handoff.State()
	if state.Phase != ReplicaHotHandoffPhaseReady || state.AppliedFrontier != 4 || state.Lag != 0 {
		t.Fatalf("ready state = %#v, want ready at frontier 4", state)
	}
	if source.fetches != 1 || target.ready != 1 {
		t.Fatalf("fetches/ready = %d/%d, want 1/1", source.fetches, target.ready)
	}
	if source.limit != 2 {
		t.Fatalf("fetch limit = %d, want 2", source.limit)
	}
	if !reflect.DeepEqual(target.installed.Payload, []byte("warm-query-state")) {
		t.Fatalf("installed snapshot payload = %q, want warm-query-state", target.installed.Payload)
	}
	if got := []string{string(target.apply[0].Payload), string(target.apply[1].Payload)}; !reflect.DeepEqual(got, []string{"delta-3", "delta-4"}) {
		t.Fatalf("applied payloads = %#v, want delta-3/delta-4", got)
	}

	token, err := handoff.Promote(state.Generation, 9)
	if err != nil {
		t.Fatalf("Promote() error = %v", err)
	}
	if token.TargetID != "replica-b" || token.Frontier != 4 || token.FencingToken != 9 {
		t.Fatalf("promotion token = %#v, want replica-b frontier 4 token 9", token)
	}
	if got := handoff.State().Phase; got != ReplicaHotHandoffPhasePromoted {
		t.Fatalf("phase after promote = %v, want promoted", got)
	}
}

func TestMZ021ReplicaHotHandoffRejectsEpochGapAndDoesNotApply(t *testing.T) {
	tests := []struct {
		name  string
		batch ReplicaHotHandoffBatch
		want  error
	}{
		{
			name: "epoch mismatch",
			batch: ReplicaHotHandoffBatch{
				Epoch:          5,
				SourceFrontier: 2,
				Deltas:         []ReplicaHotHandoffDelta{{Sequence: 2, Payload: []byte("wrong-epoch")}},
			},
			want: ErrReplicaHotHandoffEpochMismatch,
		},
		{
			name: "sequence gap",
			batch: ReplicaHotHandoffBatch{
				Epoch:          4,
				SourceFrontier: 3,
				Deltas:         []ReplicaHotHandoffDelta{{Sequence: 3, Payload: []byte("gap")}},
			},
			want: ErrReplicaHotHandoffSequenceGap,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &mz021ReplicaHandoffTestSource{
				snapshot: ReplicaHotHandoffSnapshot{SourceID: "primary", SnapshotID: "snapshot-1", Epoch: 4, Frontier: 1},
				batches:  map[uint64]ReplicaHotHandoffBatch{1: test.batch},
			}
			target := &mz021ReplicaHandoffTestTarget{}
			handoff := newMZ021ReplicaHotHandoffTest(t, source, target)
			err := handoff.SyncOnce(context.Background(), source, target)
			if !errors.Is(err, test.want) {
				t.Fatalf("SyncOnce() error = %v, want %v", err, test.want)
			}
			if len(target.apply) != 0 {
				t.Fatalf("applied deltas = %#v, want none", target.apply)
			}
			if got := handoff.State().Phase; got != ReplicaHotHandoffPhaseFailed {
				t.Fatalf("phase = %v, want failed", got)
			}
		})
	}
}

func TestMZ021ReplicaHotHandoffRejectsStalePromotionAndFencing(t *testing.T) {
	source := &mz021ReplicaHandoffTestSource{
		snapshot: ReplicaHotHandoffSnapshot{SourceID: "primary", SnapshotID: "snapshot-1", Epoch: 2, Frontier: 0},
		batches:  map[uint64]ReplicaHotHandoffBatch{0: {Epoch: 2, SourceFrontier: 0}},
	}
	target := &mz021ReplicaHandoffTestTarget{}
	handoff := newMZ021ReplicaHotHandoffTest(t, source, target)
	if err := handoff.CatchUp(context.Background(), source, target); err != nil {
		t.Fatalf("CatchUp() error = %v", err)
	}
	state := handoff.State()
	if _, err := handoff.Promote(state.Generation-1, 9); !errors.Is(err, ErrReplicaHotHandoffGeneration) {
		t.Fatalf("stale generation error = %v, want generation error", err)
	}
	if _, err := handoff.Promote(state.Generation, 8); !errors.Is(err, ErrReplicaHotHandoffFencing) {
		t.Fatalf("stale fencing error = %v, want fencing error", err)
	}
	if _, err := handoff.Promote(state.Generation, 9); err != nil {
		t.Fatalf("valid Promote() error = %v", err)
	}
}

func TestMZ021ReplicaHotHandoffRejectsInvalidSnapshotBeforeInstall(t *testing.T) {
	tests := []struct {
		name     string
		snapshot ReplicaHotHandoffSnapshot
		options  ReplicaHotHandoffOptions
		want     error
	}{
		{
			name:     "schema mismatch",
			snapshot: ReplicaHotHandoffSnapshot{SourceID: "primary", SnapshotID: "snapshot", Epoch: 1, SchemaVersion: 3},
			options:  ReplicaHotHandoffOptions{ExpectedSchemaVersion: 4},
			want:     ErrReplicaHotHandoffSchemaMismatch,
		},
		{
			name:     "snapshot too large",
			snapshot: ReplicaHotHandoffSnapshot{SourceID: "primary", SnapshotID: "snapshot", Epoch: 1, Payload: []byte("too-large")},
			options:  ReplicaHotHandoffOptions{MaxSnapshotBytes: 3},
			want:     ErrReplicaHotHandoffSnapshotTooLarge,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.options.SourceID = "primary"
			test.options.TargetID = "replica-b"
			test.options.FencingToken = 1
			handoff, err := NewReplicaHotHandoff(test.options)
			if err != nil {
				t.Fatalf("NewReplicaHotHandoff() error = %v", err)
			}
			source := &mz021ReplicaHandoffTestSource{snapshot: test.snapshot}
			target := &mz021ReplicaHandoffTestTarget{}
			err = handoff.Prepare(context.Background(), source, target)
			if !errors.Is(err, test.want) {
				t.Fatalf("Prepare() error = %v, want %v", err, test.want)
			}
			if target.installed.SnapshotID != "" || handoff.State().Phase != ReplicaHotHandoffPhaseFailed {
				t.Fatalf("target/state = %#v/%#v, want no install and failed", target.installed, handoff.State())
			}
		})
	}
}

func TestMZ021ReplicaHotHandoffRejectsEmptyBatchThatHidesFrontier(t *testing.T) {
	source := &mz021ReplicaHandoffTestSource{
		snapshot: ReplicaHotHandoffSnapshot{SourceID: "primary", SnapshotID: "snapshot", Epoch: 1, Frontier: 1},
		batches:  map[uint64]ReplicaHotHandoffBatch{1: {Epoch: 1, SourceFrontier: 2}},
	}
	target := &mz021ReplicaHandoffTestTarget{}
	handoff := newMZ021ReplicaHotHandoffTest(t, source, target)
	err := handoff.SyncOnce(context.Background(), source, target)
	if !errors.Is(err, ErrReplicaHotHandoffSequenceGap) {
		t.Fatalf("SyncOnce() error = %v, want sequence gap", err)
	}
	if len(target.apply) != 0 || target.ready != 0 {
		t.Fatalf("target apply/ready = %d/%d, want 0/0", len(target.apply), target.ready)
	}
}

func TestMZ021ReplicaHotHandoffPropagatesTargetReadinessFailure(t *testing.T) {
	source := &mz021ReplicaHandoffTestSource{
		snapshot: ReplicaHotHandoffSnapshot{SourceID: "primary", SnapshotID: "snapshot", Epoch: 1},
		batches:  map[uint64]ReplicaHotHandoffBatch{0: {Epoch: 1, SourceFrontier: 0}},
	}
	targetErr := errors.New("query state is still warming")
	target := &mz021ReplicaHandoffTestTarget{readyErr: targetErr}
	handoff := newMZ021ReplicaHotHandoffTest(t, source, target)
	err := handoff.CatchUp(context.Background(), source, target)
	if !errors.Is(err, targetErr) {
		t.Fatalf("CatchUp() error = %v, want target error", err)
	}
	if got := handoff.State().Phase; got != ReplicaHotHandoffPhaseFailed {
		t.Fatalf("phase = %v, want failed", got)
	}
}

func TestMZ021ReplicaHotHandoffCancellationDoesNotApply(t *testing.T) {
	source := &mz021ReplicaHandoffTestSource{
		snapshot: ReplicaHotHandoffSnapshot{SourceID: "primary", SnapshotID: "snapshot", Epoch: 1},
		batches: map[uint64]ReplicaHotHandoffBatch{
			0: {Epoch: 1, SourceFrontier: 1, Deltas: []ReplicaHotHandoffDelta{{Sequence: 1, Payload: []byte("delta")}}},
		},
	}
	target := &mz021ReplicaHandoffTestTarget{}
	handoff := newMZ021ReplicaHotHandoffTest(t, source, target)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := handoff.SyncOnce(ctx, source, target)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SyncOnce() error = %v, want context.Canceled", err)
	}
	if len(target.apply) != 0 || handoff.State().Phase != ReplicaHotHandoffPhaseFailed {
		t.Fatalf("target/state = %d/%v, want no apply and failed", len(target.apply), handoff.State().Phase)
	}
}

func TestMZ021ReplicaHotHandoffDefaultsAreBounded(t *testing.T) {
	handoff, err := NewReplicaHotHandoff(ReplicaHotHandoffOptions{SourceID: "primary", TargetID: "replica-b", FencingToken: 1})
	if err != nil {
		t.Fatalf("NewReplicaHotHandoff() error = %v", err)
	}
	if handoff.options.BatchSize != DefaultReplicaHotHandoffBatchSize || handoff.options.PollInterval != DefaultReplicaHotHandoffPollInterval || handoff.options.MaxSnapshotBytes != DefaultReplicaHotHandoffSnapshotBytes {
		t.Fatalf("defaults = %#v, want bounded default options", handoff.options)
	}
}
