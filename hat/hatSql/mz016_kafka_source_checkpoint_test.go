package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type mz016CheckpointStore struct {
	snapshot KafkaTableSourceSnapshot
	found    bool
	commits  int
	err      error
}

func (store *mz016CheckpointStore) Load(_ context.Context, source string) (KafkaTableSourceSnapshot, bool, error) {
	if store.err != nil {
		return KafkaTableSourceSnapshot{}, false, store.err
	}
	if !store.found || store.snapshot.Source != source {
		return KafkaTableSourceSnapshot{}, false, nil
	}
	return store.snapshot, true, nil
}

func (store *mz016CheckpointStore) Commit(_ context.Context, snapshot KafkaTableSourceSnapshot) error {
	store.commits++
	if store.err != nil {
		return store.err
	}
	store.snapshot = snapshot
	store.found = true
	return nil
}

type mz016Consumer struct {
	batch          KafkaTableBatch
	store          *mz016CheckpointStore
	commitCalls    int
	commitObserved bool
	err            error
}

func (consumer *mz016Consumer) Poll(context.Context) (KafkaTableBatch, error) {
	return consumer.batch, nil
}

func (consumer *mz016Consumer) Commit(_ context.Context, _ []SQLSourceOffset) error {
	consumer.commitCalls++
	consumer.commitObserved = consumer.store.commits > 0
	return consumer.err
}

func mz016NewSource(t *testing.T) *KafkaTableSource {
	t.Helper()
	source, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source:  "checkpointed",
		Table:   "orders",
		Topic:   "orders",
		Decoder: KafkaTableJSONDecoder,
	})
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func mz016Batch(offset uint64, transactionID string) KafkaTableBatch {
	return KafkaTableBatch{
		TransactionID: transactionID,
		Messages: []KafkaTableMessage{{
			Topic: "orders", Partition: "0", Offset: offset, Key: "order-1",
			Value: []byte(`{"id":1,"state":"paid"}`),
		}},
	}
}

func TestMZ016KafkaSourceCheckpointPersistsAndRestoresCoupledState(t *testing.T) {
	store := &mz016CheckpointStore{}
	source := mz016NewSource(t)
	result, err := source.ApplyBatchWithCheckpoint(context.Background(), mz016Batch(7, "tx-7"), store)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed || result.AppliedMessages != 1 || store.commits != 1 {
		t.Fatalf("result = %#v, commits = %d", result, store.commits)
	}
	if len(store.snapshot.Rows) != 1 || len(store.snapshot.Offsets) != 1 || len(store.snapshot.Ingestions) != 1 {
		t.Fatalf("snapshot = %#v", store.snapshot)
	}

	restored := mz016NewSource(t)
	found, err := restored.RestoreCheckpoint(context.Background(), store)
	if err != nil || !found {
		t.Fatalf("restore found = %v, err = %v", found, err)
	}
	rows, err := restored.ResolveSQLSource("KAFKA", "orders")
	if err != nil || len(rows) != 1 || rows[0]["state"] != "paid" {
		t.Fatalf("restored rows = %#v, err = %v", rows, err)
	}
	if !reflect.DeepEqual(restored.Offsets(), []SQLSourceOffset{{Source: "checkpointed", Partition: "0", Offset: 7}}) {
		t.Fatalf("restored offsets = %#v", restored.Offsets())
	}
	duplicate, err := restored.ApplyBatchWithCheckpoint(context.Background(), mz016Batch(7, "tx-7"), store)
	if err != nil || !duplicate.Duplicate || duplicate.AppliedMessages != 0 {
		t.Fatalf("duplicate = %#v, err = %v", duplicate, err)
	}
}

func TestMZ016KafkaSourceCheckpointRollsBackOnStoreFailure(t *testing.T) {
	store := &mz016CheckpointStore{}
	source := mz016NewSource(t)
	if _, err := source.ApplyBatchWithCheckpoint(context.Background(), mz016Batch(7, "tx-7"), store); err != nil {
		t.Fatal(err)
	}
	store.err = errors.New("checkpoint unavailable")
	_, err := source.ApplyBatchWithCheckpoint(context.Background(), mz016Batch(8, "tx-8"), store)
	if !errors.Is(err, ErrKafkaTableSourceCheckpointCommit) {
		t.Fatalf("error = %v, want checkpoint commit error", err)
	}
	rows, err := source.ResolveSQLSource("KAFKA", "orders")
	if err != nil || len(rows) != 1 || len(source.Offsets()) != 1 || source.Offsets()[0].Offset != 7 || source.Stats().AppliedBatches != 1 {
		t.Fatalf("rolled back rows = %#v, offsets = %#v, err = %v", rows, source.Offsets(), err)
	}
}

func TestMZ016KafkaSourceCheckpointCommitPrecedesConsumerCommit(t *testing.T) {
	store := &mz016CheckpointStore{}
	consumer := &mz016Consumer{batch: mz016Batch(9, "tx-9"), store: store, err: errors.New("broker unavailable")}
	source := mz016NewSource(t)
	result, err := source.ConsumeOnceWithCheckpoint(context.Background(), consumer, store)
	if !errors.Is(err, consumer.err) || result.AppliedMessages != 1 || consumer.commitCalls != 1 || !consumer.commitObserved {
		t.Fatalf("result = %#v, err = %v, consumer = %#v", result, err, consumer)
	}
	restored := mz016NewSource(t)
	found, err := restored.RestoreCheckpoint(context.Background(), store)
	if err != nil || !found {
		t.Fatalf("restore after broker failure found = %v, err = %v", found, err)
	}
	rows, err := restored.ResolveSQLSource("KAFKA", "orders")
	if err != nil || len(rows) != 1 {
		t.Fatalf("durable rows = %#v, err = %v", rows, err)
	}
}

func TestMZ016KafkaSourceCheckpointValidatesStoreAndContext(t *testing.T) {
	source := mz016NewSource(t)
	if _, err := source.ApplyBatchWithCheckpoint(context.Background(), mz016Batch(1, "tx-1"), nil); !errors.Is(err, ErrKafkaTableSourceCheckpointStoreRequired) {
		t.Fatalf("nil store error = %v", err)
	}
	if _, err := source.RestoreCheckpoint(context.Background(), nil); !errors.Is(err, ErrKafkaTableSourceCheckpointStoreRequired) {
		t.Fatalf("nil restore store error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.ApplyBatchWithCheckpoint(ctx, mz016Batch(1, "tx-1"), &mz016CheckpointStore{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
}
