package hatReplication

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestSpaceChangefeedBatchesSchemaAndPayloadsAtomically(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: 7,
		Capacity:      8,
	})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	key := []byte("order-1")
	after := []byte("v1")
	changes, err := feed.AppendBatch([]SpaceChange{
		{SchemaVersion: 7, Operation: SpaceChangeInsert, Key: key, After: after},
		{SchemaVersion: 7, Operation: SpaceChangeUpdate, Key: []byte("order-1"), Before: []byte("v1"), After: []byte("v2")},
		{SchemaVersion: 7, Operation: SpaceChangeDelete, Key: []byte("order-1"), Before: []byte("v2")},
	})
	if err != nil {
		t.Fatalf("AppendBatch() error = %v", err)
	}
	if len(changes) != 3 || changes[0].Sequence != 1 || changes[2].Sequence != 3 {
		t.Fatalf("changes = %#v", changes)
	}
	key[0] = 'X'
	after[0] = 'X'
	page, err := feed.ReadAfter(0, 8)
	if err != nil {
		t.Fatalf("ReadAfter() error = %v", err)
	}
	if len(page.Events) != 3 || string(page.Events[0].Key) != "order-1" || string(page.Events[0].After) != "v1" {
		t.Fatalf("page = %#v", page)
	}
	checkpoint, err := feed.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
	if checkpoint.Source != "orders" || checkpoint.Sequence != 3 {
		t.Fatalf("checkpoint = %#v", checkpoint)
	}
	if _, err := feed.AppendBatch([]SpaceChange{
		{SchemaVersion: 7, Operation: SpaceChangeInsert, Key: []byte("order-2"), After: []byte("v1")},
		{SchemaVersion: 8, Operation: SpaceChangeInsert, Key: []byte("order-3"), After: []byte("v1")},
	}); !errors.Is(err, ErrSpaceChangefeedSchemaMismatch) {
		t.Fatalf("schema mismatch error = %v, want %v", err, ErrSpaceChangefeedSchemaMismatch)
	}
	unchanged, err := feed.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() after rejected batch error = %v", err)
	}
	if !reflect.DeepEqual(unchanged, page.Events) {
		t.Fatalf("feed changed after rejected batch: got %#v want %#v", unchanged, page.Events)
	}
}

func TestSpaceChangefeedRetentionCheckpointAndWait(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: 1,
		Capacity:      2,
	})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	appendInsert := func(key string) {
		t.Helper()
		if _, err := feed.Append(SpaceChange{SchemaVersion: 1, Operation: SpaceChangeInsert, Key: []byte(key), After: []byte("row")}); err != nil {
			t.Fatalf("Append(%q) error = %v", key, err)
		}
	}
	appendInsert("one")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := feed.WaitAfter(ctx, 1, 8); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitAfter() error = %v, want deadline", err)
	}
	waitResult := make(chan struct {
		page SpaceChangefeedPage
		err  error
	}, 1)
	go func() {
		page, err := feed.WaitAfter(context.Background(), 1, 8)
		waitResult <- struct {
			page SpaceChangefeedPage
			err  error
		}{page: page, err: err}
	}()
	appendInsert("two")
	select {
	case result := <-waitResult:
		if result.err != nil || len(result.page.Events) != 1 || result.page.Events[0].Sequence != 2 {
			t.Fatalf("WaitAfter() result = %#v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitAfter() did not wake after Append()")
	}
	appendInsert("three")
	if _, err := feed.ReadAfter(0, 8); !errors.Is(err, ErrSpaceChangefeedHistoryGap) {
		t.Fatalf("ReadAfter() gap error = %v, want %v", err, ErrSpaceChangefeedHistoryGap)
	}
	checkpoint, err := NewChangefeedCheckpoint("orders", 1)
	if err != nil {
		t.Fatalf("NewChangefeedCheckpoint() error = %v", err)
	}
	page, err := feed.ReadFromCheckpoint(checkpoint, 8)
	if err != nil {
		t.Fatalf("ReadFromCheckpoint() error = %v", err)
	}
	if len(page.Events) != 2 || page.Events[0].Sequence != 2 || page.Events[1].Sequence != 3 {
		t.Fatalf("checkpoint page = %#v", page)
	}
	wrongSource, err := NewChangefeedCheckpoint("customers", 1)
	if err != nil {
		t.Fatalf("NewChangefeedCheckpoint(wrong source) error = %v", err)
	}
	if _, err := feed.ReadFromCheckpoint(wrongSource, 8); !errors.Is(err, ErrSpaceChangefeedCheckpointSourceMismatch) {
		t.Fatalf("wrong checkpoint source error = %v, want %v", err, ErrSpaceChangefeedCheckpointSourceMismatch)
	}
}

func TestSpaceChangefeedRejectsInvalidChangesAndWakesOnClose(t *testing.T) {
	if _, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders"}); !errors.Is(err, ErrSpaceChangefeedSchemaRequired) {
		t.Fatalf("missing schema error = %v, want %v", err, ErrSpaceChangefeedSchemaRequired)
	}
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: 1})
	if err != nil {
		t.Fatalf("NewSpaceChangefeed() error = %v", err)
	}
	if _, err := feed.Append(SpaceChange{SchemaVersion: 1, Operation: SpaceChangeUpdate, Key: []byte("id"), Before: []byte("old")}); !errors.Is(err, ErrSpaceChangefeedChangeInvalid) {
		t.Fatalf("incomplete update error = %v, want %v", err, ErrSpaceChangefeedChangeInvalid)
	}
	closed := make(chan error, 1)
	go func() {
		_, waitErr := feed.WaitAfter(context.Background(), 0, 1)
		closed <- waitErr
	}()
	if err := feed.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case waitErr := <-closed:
		if !errors.Is(waitErr, ErrSpaceChangefeedClosed) {
			t.Fatalf("closed WaitAfter() error = %v, want %v", waitErr, ErrSpaceChangefeedClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("closed WaitAfter() did not wake")
	}
}
