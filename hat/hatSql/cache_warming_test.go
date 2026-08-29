package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestCacheWarmerWarmsHotTargetsOnceAndAfterInvalidation(t *testing.T) {
	warmer := NewCacheWarmer(CacheWarmOptions{Threshold: 3, Capacity: 4})
	target := CacheWarmTarget{Kind: CacheWarmView, Name: "daily-sales"}
	calls := 0
	if err := warmer.Register(target, func(context.Context) error {
		calls++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for access := 0; access < 2; access++ {
		warmed, err := warmer.Observe(context.Background(), target)
		if err != nil || warmed {
			t.Fatalf("Observe before threshold = warmed:%v err:%v", warmed, err)
		}
	}
	if warmed, err := warmer.Observe(context.Background(), target); err != nil || !warmed || calls != 1 {
		t.Fatalf("Observe at threshold = warmed:%v err:%v calls:%d", warmed, err, calls)
	}
	if warmed, err := warmer.Observe(context.Background(), target); err != nil || warmed || calls != 1 {
		t.Fatalf("Observe after warm = warmed:%v err:%v calls:%d", warmed, err, calls)
	}
	warmer.Invalidate(target)
	for access := 0; access < 3; access++ {
		if _, err := warmer.Observe(context.Background(), target); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("calls after invalidation = %d, want 2", calls)
	}
}

func TestCacheWarmerRetriesFailuresAndValidatesTargets(t *testing.T) {
	warmer := NewCacheWarmer(CacheWarmOptions{Threshold: 1, Capacity: 1})
	target := CacheWarmTarget{Kind: CacheWarmIndex, Name: "orders.customer"}
	fail := true
	if err := warmer.Register(target, func(context.Context) error {
		if fail {
			fail = false
			return errors.New("temporary")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if warmed, err := warmer.Observe(context.Background(), target); !warmed || err == nil {
		t.Fatalf("failed warm = warmed:%v err:%v", warmed, err)
	}
	if warmed, err := warmer.Observe(context.Background(), target); !warmed || err != nil {
		t.Fatalf("retried warm = warmed:%v err:%v", warmed, err)
	}
	if err := warmer.Register(CacheWarmTarget{Kind: CacheWarmIndex}, func(context.Context) error { return nil }); err == nil {
		t.Fatal("Register accepted empty target name")
	}
}
