package hatPipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestFrontierRegistryAdvancesAndSnapshots(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders"); !errors.Is(err, ErrFrontierAlreadyRegistered) {
		t.Fatalf("duplicate registration error = %v", err)
	}
	if err := registry.Advance("orders", 10, 20); err != nil {
		t.Fatal(err)
	}
	if err := registry.Advance("orders", 10, 20); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := registry.Snapshot("orders")
	if !ok {
		t.Fatal("missing frontier snapshot")
	}
	if snapshot.Lower != 10 || snapshot.Upper != 20 || snapshot.Generation != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if !snapshot.UpdatedAt.Before(time.Now().Add(time.Second)) {
		t.Fatalf("snapshot timestamp = %s", snapshot.UpdatedAt)
	}
	if !registry.Covers("orders", 10) || registry.Covers("orders", 11) {
		t.Fatal("unexpected Covers result")
	}
	if got := registry.SnapshotAll(); len(got) != 1 || got[0].ID != "orders" {
		t.Fatalf("all snapshots = %+v", got)
	}
}

func TestFrontierRegistryRejectsInvalidAdvances(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("source"); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		lower uint64
		upper uint64
		want  error
	}{
		{name: "lower ahead of upper", lower: 5, upper: 4, want: ErrFrontierOrderInvalid},
		{name: "lower regression", lower: 0, upper: 1, want: ErrFrontierRegression},
		{name: "upper regression", lower: 0, upper: 0, want: ErrFrontierRegression},
	}
	if err := registry.Advance("source", 2, 4); err != nil {
		t.Fatal(err)
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := registry.Advance("source", test.lower, test.upper); !errors.Is(err, test.want) {
				t.Fatalf("Advance() error = %v, want %v", err, test.want)
			}
		})
	}
	if err := registry.Advance("missing", 1, 1); !errors.Is(err, ErrFrontierNotFound) {
		t.Fatalf("missing advance error = %v", err)
	}
	if !errors.Is(registry.Register(""), ErrFrontierIDEmpty) {
		t.Fatal("empty ID did not fail")
	}
}

func TestFrontierRegistryWaitsForLowerFrontierAndHonorsCancellation(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("events"); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- registry.WaitUntil(context.Background(), "events", 7) }()
	select {
	case err := <-result:
		t.Fatalf("WaitUntil returned before advance: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := registry.Advance("events", 7, 9); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := registry.WaitUntil(ctx, "events", 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled WaitUntil error = %v", err)
	}
	if err := registry.WaitUntil(context.Background(), "missing", 1); !errors.Is(err, ErrFrontierNotFound) {
		t.Fatalf("missing WaitUntil error = %v", err)
	}
}

func TestFrontierRegistryWakesAllWaiters(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("all"); err != nil {
		t.Fatal(err)
	}
	results := []chan error{make(chan error, 1), make(chan error, 1)}
	for _, result := range results {
		go func(result chan error) {
			result <- registry.WaitUntil(context.Background(), "all", 1)
		}(result)
	}
	time.Sleep(10 * time.Millisecond)
	if err := registry.Advance("all", 1, 1); err != nil {
		t.Fatal(err)
	}
	for i, result := range results {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("waiter %d error = %v", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("waiter %d did not wake", i)
		}
	}
}

func TestFrontierRegistryBoundsObjectsAndUnregisters(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"b", "a"} {
		if err := registry.Register(id); err != nil {
			t.Fatal(err)
		}
	}
	if !errors.Is(registry.Register("c"), ErrFrontierObjectLimit) {
		t.Fatal("object limit was not enforced")
	}
	if got := registry.SnapshotAll(); !reflect.DeepEqual([]string{got[0].ID, got[1].ID}, []string{"a", "b"}) {
		t.Fatalf("snapshot order = %+v", got)
	}
	if err := registry.Unregister("a"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("c"); err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.Snapshot("a"); ok {
		t.Fatal("unregistered frontier still exists")
	}
}

func TestFrontierRegistryCloseWakesWaitersAndRejectsChanges(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("close"); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- registry.WaitUntil(context.Background(), "close", 1) }()
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrFrontierClosed) {
		t.Fatalf("wait after close error = %v", err)
	}
	if err := registry.Register("after"); !errors.Is(err, ErrFrontierClosed) {
		t.Fatalf("register after close error = %v", err)
	}
	if err := registry.Advance("close", 1, 1); !errors.Is(err, ErrFrontierClosed) {
		t.Fatalf("advance after close error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFrontierRegistryOptionValidation(t *testing.T) {
	if _, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: -1}); !errors.Is(err, ErrFrontierOptionsInvalid) {
		t.Fatalf("negative max objects error = %v", err)
	}
	if _, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: maxFrontierObjects + 1}); !errors.Is(err, ErrFrontierOptionsInvalid) {
		t.Fatalf("oversized max objects error = %v", err)
	}
	var nilRegistry *FrontierRegistry
	if err := nilRegistry.Register("nil"); !errors.Is(err, ErrFrontierClosed) {
		t.Fatalf("nil register error = %v", err)
	}
	if _, ok := nilRegistry.Snapshot("nil"); ok {
		t.Fatal("nil snapshot unexpectedly exists")
	}
}
