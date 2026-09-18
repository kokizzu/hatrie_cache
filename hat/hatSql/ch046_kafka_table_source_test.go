package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestKafkaTableSourceApplyBatchAndResolve(t *testing.T) {
	source, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source:  "orders-source",
		Table:   "orders",
		Topic:   "orders",
		Decoder: KafkaTableJSONDecoder,
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := source.ApplyBatch(KafkaTableBatch{
		TransactionID: "tx-1",
		Messages: []KafkaTableMessage{
			{Topic: "orders", Partition: "0", Offset: 1, Key: "a", Value: []byte(`{"id":"a","status":"new"}`)},
			{Topic: "orders", Partition: "0", Offset: 2, Key: "b", Value: []byte(`{"id":"b","status":"new"}`)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Committed || first.Duplicate || first.AppliedMessages != 2 {
		t.Fatalf("first result = %#v", first)
	}

	second, err := source.ApplyBatch(KafkaTableBatch{
		TransactionID: "tx-2",
		Messages: []KafkaTableMessage{
			{Topic: "orders", Partition: "0", Offset: 3, Key: "a", Value: []byte(`{"id":"a","status":"paid"}`)},
			{Topic: "orders", Partition: "0", Offset: 4, Key: "b"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.AppliedMessages != 2 {
		t.Fatalf("second result = %#v", second)
	}

	rows, err := source.ResolveSQLSource("KAFKA", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["id"] != "a" || rows[0]["status"] != "paid" {
		t.Fatalf("rows = %#v", rows)
	}
	offsets := source.Offsets()
	wantOffsets := []SQLSourceOffset{{Source: "orders-source", Partition: "0", Offset: 4}}
	if !reflect.DeepEqual(offsets, wantOffsets) {
		t.Fatalf("offsets = %#v, want %#v", offsets, wantOffsets)
	}
}

func TestKafkaTableSourceReplayAndFailedBatchAreSafe(t *testing.T) {
	fail := true
	source, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source: "orders-source",
		Table:  "orders",
		Topic:  "orders",
		Decoder: func(message KafkaTableMessage) (KafkaTableChange, error) {
			if fail && message.Key == "bad" {
				return KafkaTableChange{}, errors.New("decoder rejected record")
			}
			return KafkaTableChange{Key: message.Key, Operation: KafkaTableUpsert, Row: Row{"id": message.Key}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := source.ApplyBatch(KafkaTableBatch{
		TransactionID: "tx-failed",
		Messages: []KafkaTableMessage{
			{Topic: "orders", Partition: "0", Offset: 1, Key: "good"},
			{Topic: "orders", Partition: "0", Offset: 2, Key: "bad"},
		},
	}); err == nil {
		t.Fatal("expected decoder failure")
	}
	if got := source.Offsets(); len(got) != 0 {
		t.Fatalf("failed batch advanced offsets: %#v", got)
	}
	if rows, err := source.ResolveSQLSource("KAFKA", "orders"); err != nil || len(rows) != 0 {
		t.Fatalf("failed batch rows = %#v, %v", rows, err)
	}

	fail = false
	if _, err := source.ApplyBatch(KafkaTableBatch{
		TransactionID: "tx-failed",
		Messages: []KafkaTableMessage{
			{Topic: "orders", Partition: "0", Offset: 1, Key: "good"},
			{Topic: "orders", Partition: "0", Offset: 2, Key: "bad"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := source.ApplyBatch(KafkaTableBatch{
		TransactionID: "tx-failed",
		Messages: []KafkaTableMessage{
			{Topic: "orders", Partition: "0", Offset: 1, Key: "good"},
			{Topic: "orders", Partition: "0", Offset: 2, Key: "bad"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Duplicate || !result.Committed || result.AppliedMessages != 0 {
		t.Fatalf("replay result = %#v", result)
	}
}

func TestKafkaTableSourceSnapshotRestoreAndConsumerCommit(t *testing.T) {
	source, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source:  "orders-source",
		Table:   "orders",
		Topic:   "orders",
		Decoder: KafkaTableJSONDecoder,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ApplyBatch(KafkaTableBatch{
		TransactionID: "tx-1",
		Messages:      []KafkaTableMessage{{Topic: "orders", Partition: "0", Offset: 1, Key: "a", Value: []byte(`{"id":"a"}`)}},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := source.Snapshot()

	restored, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source:  "orders-source",
		Table:   "orders",
		Topic:   "orders",
		Decoder: KafkaTableJSONDecoder,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	if result, err := restored.ApplyBatch(KafkaTableBatch{
		TransactionID: "tx-1",
		Messages:      []KafkaTableMessage{{Topic: "orders", Partition: "0", Offset: 1, Key: "a", Value: []byte(`{"id":"changed"}`)}},
	}); err != nil {
		t.Fatal(err)
	} else if !result.Duplicate {
		t.Fatalf("restored replay result = %#v", result)
	}

	consumer := &ch046TestConsumer{batch: KafkaTableBatch{
		TransactionID: "tx-2",
		Messages:      []KafkaTableMessage{{Topic: "orders", Partition: "0", Offset: 2, Key: "b", Value: []byte(`{"id":"b"}`)}},
	}}
	result, err := restored.ConsumeOnce(context.Background(), consumer)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed || len(consumer.commits) != 1 || consumer.commits[0][0].Offset != 2 {
		t.Fatalf("consume result = %#v, commits = %#v", result, consumer.commits)
	}
}

func TestKafkaTableSourceValidationAndCommitOrdering(t *testing.T) {
	source, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source:           "orders-source",
		Table:            "orders",
		Topic:            "orders",
		MaxBatchMessages: 1,
		Decoder:          KafkaTableJSONDecoder,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer := &ch046TestConsumer{batch: KafkaTableBatch{Messages: []KafkaTableMessage{
		{Topic: "orders", Partition: "0", Offset: 1, Key: "a", Value: []byte(`{"id":"a"}`)},
		{Topic: "orders", Partition: "0", Offset: 2, Key: "b", Value: []byte(`{"id":"b"}`)},
	}}}
	if _, err := source.ConsumeOnce(context.Background(), consumer); !errors.Is(err, ErrKafkaTableSourceBatchTooLarge) {
		t.Fatalf("oversized batch error = %v", err)
	}
	if len(consumer.commits) != 0 {
		t.Fatalf("oversized batch committed: %#v", consumer.commits)
	}

	if _, err := source.ApplyBatch(KafkaTableBatch{Messages: []KafkaTableMessage{{
		Topic: "wrong-topic", Partition: "0", Offset: 1, Key: "a", Value: []byte(`{"id":"a"}`),
	}}}); !errors.Is(err, ErrKafkaTableSourceMessageInvalid) {
		t.Fatalf("wrong topic error = %v", err)
	}
}

func TestKafkaTableSourceAcceptsOffsetZero(t *testing.T) {
	source, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source:  "orders-source",
		Table:   "orders",
		Topic:   "orders",
		Decoder: KafkaTableJSONDecoder,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.ApplyBatch(KafkaTableBatch{
		TransactionID: "tx-zero",
		Messages:      []KafkaTableMessage{{Topic: "orders", Partition: "0", Offset: 0, Key: "a", Value: []byte(`{"id":"a"}`)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AppliedMessages != 1 {
		t.Fatalf("result = %#v", result)
	}
	if rows, err := source.ResolveSQLSource("KAFKA", "orders"); err != nil || len(rows) != 1 {
		t.Fatalf("rows = %#v, %v", rows, err)
	}
}

type ch046TestConsumer struct {
	batch   KafkaTableBatch
	err     error
	commits [][]SQLSourceOffset
}

func (consumer *ch046TestConsumer) Poll(context.Context) (KafkaTableBatch, error) {
	return consumer.batch, consumer.err
}

func (consumer *ch046TestConsumer) Commit(_ context.Context, offsets []SQLSourceOffset) error {
	consumer.commits = append(consumer.commits, offsets)
	return nil
}
