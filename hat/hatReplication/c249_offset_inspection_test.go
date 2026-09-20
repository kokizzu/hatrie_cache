package hatReplication

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestSpaceChangefeedInspectDoesNotCreateConsumerOrAdvanceCheckpoint(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: "v1",
		MaxEvents:     8,
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 4; index++ {
		if _, err := feed.Publish(SpaceChangefeedEvent{
			Operation: SpaceChangefeedUpsert,
			Key:       []byte{byte(index)},
			After:     []byte("value"),
		}); err != nil {
			t.Fatal(err)
		}
	}

	before := feed.Stats()
	inspection, err := feed.Inspect(context.Background(), SpaceChangefeedInspectOptions{
		AfterSequence: 1,
		MaxEvents:     2,
	})
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if inspection.FirstAvailableSequence != 1 || inspection.NextSequence != 5 || !inspection.More {
		t.Fatalf("inspection bounds = %#v, want first=1 next=5 more=true", inspection)
	}
	if len(inspection.Events) != 2 || inspection.Events[0].Sequence != 2 || inspection.Events[1].Sequence != 3 {
		t.Fatalf("inspection events = %#v, want sequences 2 and 3", inspection.Events)
	}
	if after := feed.Stats(); after.Subscribers != before.Subscribers || after.Published != before.Published {
		t.Fatalf("Inspect() changed stats from %#v to %#v", before, after)
	}

	inspection.Events[0].Key[0] = 99
	repeated, err := feed.Inspect(context.Background(), SpaceChangefeedInspectOptions{AfterSequence: 1, MaxEvents: 2})
	if err != nil {
		t.Fatalf("repeated Inspect() error = %v", err)
	}
	if repeated.Events[0].Key[0] != 1 {
		t.Fatalf("Inspect() returned retained payload alias: %#v", repeated.Events[0].Key)
	}
}

func TestSpaceChangefeedInspectReportsGapsAheadAndBounds(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:         "orders",
		SchemaVersion: "v1",
		MaxEvents:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte{byte(index)}, After: []byte("value")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := feed.Inspect(context.Background(), SpaceChangefeedInspectOptions{}); !errors.Is(err, ErrSpaceChangefeedHistoryGap) {
		t.Fatalf("history gap error = %v, want ErrSpaceChangefeedHistoryGap", err)
	}
	if _, err := feed.Inspect(context.Background(), SpaceChangefeedInspectOptions{AfterSequence: 4}); !errors.Is(err, ErrSpaceChangefeedCheckpointAhead) {
		t.Fatalf("ahead error = %v, want ErrSpaceChangefeedCheckpointAhead", err)
	}
	if _, err := feed.Inspect(context.Background(), SpaceChangefeedInspectOptions{AfterSequence: 1, MaxEvents: 0}); err != nil {
		t.Fatalf("default bounds error = %v", err)
	}
	if _, err := feed.Inspect(context.Background(), SpaceChangefeedInspectOptions{AfterSequence: 1, MaxBytes: 1}); !errors.Is(err, ErrSpaceChangefeedInspectLimit) {
		t.Fatalf("byte bound error = %v, want ErrSpaceChangefeedInspectLimit", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := feed.Inspect(ctx, SpaceChangefeedInspectOptions{AfterSequence: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v, want context.Canceled", err)
	}
}

func TestSpaceChangefeedInspectEmptyFeedIsStable(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := feed.Inspect(context.Background(), SpaceChangefeedInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := SpaceChangefeedInspection{NextSequence: 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("empty inspection = %#v, want %#v", got, want)
	}
}

func TestSpaceChangefeedInspectRejectsInvalidLimitsAndClosedFeed(t *testing.T) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{
		Operation: SpaceChangefeedUpsert,
		Key:       []byte("k"),
		After:     []byte("value"),
	}); err != nil {
		t.Fatal(err)
	}
	for name, options := range map[string]SpaceChangefeedInspectOptions{
		"negative events": {MaxEvents: -1},
		"negative bytes":  {MaxBytes: -1},
		"too many events": {MaxEvents: MaxSpaceChangefeedInspectMaxEvents + 1},
		"too many bytes":  {MaxBytes: MaxSpaceChangefeedInspectMaxBytes + 1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := feed.Inspect(context.Background(), options); !errors.Is(err, ErrSpaceChangefeedInspectLimit) {
				t.Fatalf("Inspect() error = %v, want ErrSpaceChangefeedInspectLimit", err)
			}
		})
	}
	feed.Close()
	if _, err := feed.Inspect(context.Background(), SpaceChangefeedInspectOptions{}); !errors.Is(err, ErrSpaceChangefeedClosed) {
		t.Fatalf("closed Inspect() error = %v, want ErrSpaceChangefeedClosed", err)
	}
}
