package hatDataStructure

import (
	"errors"
	"sync"
	"testing"
)

func TestTU20OnlineSpaceUpgradeLifecycle(t *testing.T) {
	space, err := NewOnlineSpaceUpgrade[string, int](1)
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Set("a", 1); err != nil {
		t.Fatal(err)
	}
	if err := space.Set("b", 2); err != nil {
		t.Fatal(err)
	}
	if err := space.Begin(2, func(value int) (int, error) { return value * 10, nil }); err != nil {
		t.Fatal(err)
	}
	if err := space.Set("new", 3); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := space.Get("a"); err != nil || !ok || value != 10 {
		t.Fatalf("lazy get = %v, %v, %v", value, ok, err)
	}
	stats := space.Stats()
	if stats.Phase != OnlineUpgradeRunning || stats.CurrentVersion != 1 || stats.TargetVersion != 2 || stats.PendingRecords != 1 || stats.ConvertedRecords != 1 {
		t.Fatalf("unexpected active stats: %+v", stats)
	}
	if processed, remaining, err := space.UpgradeBatch(1); err != nil || processed != 1 || remaining != 0 {
		t.Fatalf("batch = %d, %d, %v", processed, remaining, err)
	}
	if err := space.Complete(); err != nil {
		t.Fatal(err)
	}
	stats = space.Stats()
	if stats.Phase != OnlineUpgradeCompleted || stats.CurrentVersion != 2 || stats.TargetVersion != 0 || stats.PendingRecords != 0 {
		t.Fatalf("unexpected completed stats: %+v", stats)
	}
	if value, ok, err := space.Get("b"); err != nil || !ok || value != 20 {
		t.Fatalf("completed get = %v, %v, %v", value, ok, err)
	}
	if err := space.Set("after", 4); err != nil {
		t.Fatal(err)
	}
}

func TestTU20OnlineSpaceUpgradePauseAndWriteCompatibility(t *testing.T) {
	space, err := NewOnlineSpaceUpgrade[string, int](3)
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Set("old", 5); err != nil {
		t.Fatal(err)
	}
	if err := space.Begin(4, func(value int) (int, error) { return value + 1, nil }); err != nil {
		t.Fatal(err)
	}
	if err := space.Pause(); err != nil {
		t.Fatal(err)
	}
	if err := space.Set("during-pause", 7); err != nil {
		t.Fatal(err)
	}
	if _, _, err := space.UpgradeBatch(1); !errors.Is(err, ErrOnlineSpaceUpgradePhase) {
		t.Fatalf("paused batch error = %v", err)
	}
	if err := space.Resume(); err != nil {
		t.Fatal(err)
	}
	if processed, remaining, err := space.UpgradeBatch(10); err != nil || processed != 1 || remaining != 0 {
		t.Fatalf("resumed batch = %d, %d, %v", processed, remaining, err)
	}
	if err := space.Complete(); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := space.Get("during-pause"); err != nil || !ok || value != 7 {
		t.Fatalf("new value = %v, %v, %v", value, ok, err)
	}
}

func TestTU20OnlineSpaceUpgradeErrorDoesNotMutate(t *testing.T) {
	space, err := NewOnlineSpaceUpgrade[string, int](1)
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Set("bad", -1); err != nil {
		t.Fatal(err)
	}
	conversionErr := errors.New("cannot convert")
	if err := space.Begin(2, func(value int) (int, error) {
		if value < 0 {
			return 0, conversionErr
		}
		return value + 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	if value, ok, err := space.Get("bad"); !errors.Is(err, conversionErr) || ok || value != 0 {
		t.Fatalf("failed lazy conversion = %v, %v, %v", value, ok, err)
	}
	if processed, remaining, err := space.UpgradeBatch(1); !errors.Is(err, conversionErr) || processed != 0 || remaining != 1 {
		t.Fatalf("failed batch = %d, %d, %v", processed, remaining, err)
	}
	if stats := space.Stats(); stats.CurrentVersion != 1 || stats.TargetVersion != 2 || stats.PendingRecords != 1 || stats.ConvertedRecords != 0 {
		t.Fatalf("failed conversion mutated state: %+v", stats)
	}
	if err := space.Complete(); !errors.Is(err, ErrOnlineSpaceUpgradePending) {
		t.Fatalf("complete with failed conversion error = %v", err)
	}
}

func TestTU20OnlineSpaceUpgradeDeleteAndValidation(t *testing.T) {
	if _, err := NewOnlineSpaceUpgrade[string, int](0); !errors.Is(err, ErrOnlineSpaceInvalidVersion) {
		t.Fatalf("invalid initial version error = %v", err)
	}
	space, err := NewOnlineSpaceUpgrade[string, int](1)
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Begin(1, func(value int) (int, error) { return value, nil }); !errors.Is(err, ErrOnlineSpaceInvalidVersion) {
		t.Fatalf("invalid target error = %v", err)
	}
	if err := space.Begin(2, nil); !errors.Is(err, ErrOnlineSpaceMissingConverter) {
		t.Fatalf("missing converter error = %v", err)
	}
	if err := space.Set("old", 1); err != nil {
		t.Fatal(err)
	}
	if err := space.Begin(2, func(value int) (int, error) { return value + 1, nil }); err != nil {
		t.Fatal(err)
	}
	if !space.Delete("old") || space.Delete("old") {
		t.Fatal("delete result mismatch")
	}
	if stats := space.Stats(); stats.PendingRecords != 0 || stats.Items != 0 {
		t.Fatalf("delete stats = %+v", stats)
	}
	if _, _, err := space.UpgradeBatch(0); !errors.Is(err, ErrOnlineSpaceInvalidBatch) {
		t.Fatalf("invalid batch error = %v", err)
	}
}

func TestTU20OnlineSpaceUpgradeNilAndConcurrent(t *testing.T) {
	var nilSpace *OnlineSpaceUpgrade[int, int]
	if _, ok, err := nilSpace.Get(1); !errors.Is(err, ErrOnlineSpaceNil) || ok {
		t.Fatalf("nil get = %v, %v", ok, err)
	}
	if err := nilSpace.Set(1, 1); !errors.Is(err, ErrOnlineSpaceNil) {
		t.Fatalf("nil set = %v", err)
	}

	space, err := NewOnlineSpaceUpgrade[int, int](1)
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Begin(2, func(value int) (int, error) { return value + 1, nil }); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				key := worker*1000 + i
				if err := space.Set(key, i); err != nil {
					t.Errorf("set: %v", err)
					return
				}
				if _, _, err := space.Get(key); err != nil {
					t.Errorf("get: %v", err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	if stats := space.Stats(); stats.Items != 8000 || stats.PendingRecords != 0 {
		t.Fatalf("concurrent stats = %+v", stats)
	}
}
