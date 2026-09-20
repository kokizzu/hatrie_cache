package hatDataStructure

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTU53TracksMonotonicSourceProgress(t *testing.T) {
	frontier, err := NewLogicalFrontier(DefaultLogicalFrontierConfig())
	if err != nil {
		t.Fatal(err)
	}

	if err := frontier.Register("eu", 10); err != nil {
		t.Fatal(err)
	}
	if err := frontier.Register("us", 20); err != nil {
		t.Fatal(err)
	}
	if got, complete := frontier.Current(); got != 10 || !complete {
		t.Fatalf("initial frontier = %d, complete=%v", got, complete)
	}

	advanced, err := frontier.Advance("us", 25)
	if err != nil || !advanced {
		t.Fatalf("advance us = %v, %v", advanced, err)
	}
	if got, _ := frontier.Current(); got != 10 {
		t.Fatalf("frontier moved past lagging source: %d", got)
	}

	advanced, err = frontier.Advance("eu", 15)
	if err != nil || !advanced {
		t.Fatalf("advance eu = %v, %v", advanced, err)
	}
	if got, _ := frontier.Current(); got != 15 {
		t.Fatalf("frontier after advance = %d", got)
	}

	advanced, err = frontier.Advance("eu", 15)
	if err != nil || advanced {
		t.Fatalf("equal advance = %v, %v", advanced, err)
	}
	if _, err := frontier.Advance("eu", 14); !errors.Is(err, ErrLogicalTimeRegression) {
		t.Fatalf("backward advance error = %v", err)
	}
}

func TestTU53WaitsForFrontierAndSupportsCancellation(t *testing.T) {
	frontier, err := NewLogicalFrontier(DefaultLogicalFrontierConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := frontier.Register("source", 1); err != nil {
		t.Fatal(err)
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- frontier.Wait(waitCtx, 5)
	}()

	select {
	case err := <-waitDone:
		t.Fatalf("wait returned before progress: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if _, err := frontier.Advance("source", 5); err != nil {
		t.Fatal(err)
	}
	if err := <-waitDone; err != nil {
		t.Fatalf("wait after progress: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := frontier.Wait(cancelCtx, 6); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait error = %v", err)
	}
}

func TestTU53BoundsSourcesAndCopiesSnapshots(t *testing.T) {
	frontier, err := NewLogicalFrontier(LogicalFrontierConfig{MaxSources: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := frontier.Register("b", 4); err != nil {
		t.Fatal(err)
	}
	if err := frontier.Register("a", 3); err != nil {
		t.Fatal(err)
	}
	if err := frontier.Register("c", 1); !errors.Is(err, ErrLogicalFrontierFull) {
		t.Fatalf("third source error = %v", err)
	}

	snapshot := frontier.Snapshot()
	if len(snapshot.Sources) != 2 || snapshot.Sources[0].Name != "a" || snapshot.Sources[1].Name != "b" {
		t.Fatalf("snapshot order = %+v", snapshot.Sources)
	}
	snapshot.Sources[0].Name = "mutated"
	if got := frontier.Snapshot().Sources[0].Name; got != "a" {
		t.Fatalf("snapshot leaked mutable source name: %q", got)
	}

	if err := frontier.Unregister("a"); err != nil {
		t.Fatal(err)
	}
	if got, _ := frontier.Current(); got != 4 {
		t.Fatalf("frontier after unregister = %d", got)
	}
	if err := frontier.Unregister("missing"); !errors.Is(err, ErrLogicalSourceNotFound) {
		t.Fatalf("missing unregister error = %v", err)
	}
}
