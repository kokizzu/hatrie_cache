package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

const ch029QuotaQuery = "FROM VALUES (1) AS values(id) SELECT id"

func TestCH029SQLQuotaRegistryEnforcesKeyedQueryWindow(t *testing.T) {
	now := time.Unix(100, 0)
	registry, err := NewSQLQuotaRegistry(SQLQuotaRegistryOptions{
		Limits:  SQLQuotaLimits{MaxQueries: 1, Window: time.Minute},
		MaxKeys: 4,
		Now:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new SQL quota registry: %v", err)
	}
	options := SQLQueryOptions{Quota: registry, QuotaKey: "alice"}
	if _, err := ExecuteSQLQueryContext(context.Background(), ch029QuotaQuery, nil, options); err != nil {
		t.Fatalf("first alice query: %v", err)
	}
	if _, err := ExecuteSQLQueryContext(context.Background(), ch029QuotaQuery, nil, options); !errors.Is(err, ErrSQLQuotaExceeded) {
		t.Fatalf("second alice query error = %v, want ErrSQLQuotaExceeded", err)
	}
	if _, err := ExecuteSQLQueryContext(context.Background(), ch029QuotaQuery, nil, SQLQueryOptions{Quota: registry, QuotaKey: "bob"}); err != nil {
		t.Fatalf("first bob query: %v", err)
	}
	now = now.Add(time.Minute)
	if _, err := ExecuteSQLQueryContext(context.Background(), ch029QuotaQuery, nil, options); err != nil {
		t.Fatalf("alice query after window: %v", err)
	}
}

func TestCH029SQLQuotaRegistryEnforcesBytesAndElapsedTime(t *testing.T) {
	registry, err := NewSQLQuotaRegistry(SQLQuotaRegistryOptions{
		Limits: SQLQuotaLimits{MaxResultBytes: 1, Window: time.Minute},
	})
	if err != nil {
		t.Fatalf("new byte quota registry: %v", err)
	}
	_, err = ExecuteSQLQueryContext(context.Background(), ch029QuotaQuery, nil, SQLQueryOptions{Quota: registry, QuotaKey: "bytes"})
	if !errors.Is(err, ErrSQLQuotaExceeded) {
		t.Fatalf("byte quota error = %v, want ErrSQLQuotaExceeded", err)
	}

	registry, err = NewSQLQuotaRegistry(SQLQuotaRegistryOptions{
		Limits: SQLQuotaLimits{MaxExecutionTime: time.Nanosecond, Window: time.Minute},
	})
	if err != nil {
		t.Fatalf("new elapsed quota registry: %v", err)
	}
	reservation, err := registry.Begin("elapsed")
	if err != nil {
		t.Fatalf("begin elapsed quota: %v", err)
	}
	if err := reservation.Finish(0, time.Second); !errors.Is(err, ErrSQLQuotaExceeded) {
		t.Fatalf("elapsed quota error = %v, want ErrSQLQuotaExceeded", err)
	}
}

func TestCH029SQLQuotaRegistryChargesStreamingRows(t *testing.T) {
	registry, err := NewSQLQuotaRegistry(SQLQuotaRegistryOptions{
		Limits: SQLQuotaLimits{MaxResultBytes: 1, Window: time.Minute},
	})
	if err != nil {
		t.Fatalf("new streaming quota registry: %v", err)
	}
	rows := 0
	err = ExecuteSQLQueryRows(context.Background(), ch029QuotaQuery, nil, nil, SQLQueryOptions{
		Quota:    registry,
		QuotaKey: "stream",
	}, func([]string, SQLRow) error {
		rows++
		return nil
	})
	if !errors.Is(err, ErrSQLQuotaExceeded) {
		t.Fatalf("streaming quota error = %v, want ErrSQLQuotaExceeded", err)
	}
	if rows != 1 {
		t.Fatalf("streaming callback rows = %d, want 1", rows)
	}
}

func TestCH029SQLQuotaRegistryBoundsKeyState(t *testing.T) {
	now := time.Unix(200, 0)
	registry, err := NewSQLQuotaRegistry(SQLQuotaRegistryOptions{
		Limits:  SQLQuotaLimits{MaxQueries: 1, Window: time.Minute},
		MaxKeys: 1,
		Now:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new bounded SQL quota registry: %v", err)
	}
	reservation, err := registry.Begin("alice")
	if err != nil {
		t.Fatalf("begin alice quota: %v", err)
	}
	if err := reservation.Finish(0, 0); err != nil {
		t.Fatalf("finish alice quota: %v", err)
	}
	if _, err := registry.Begin("bob"); !errors.Is(err, ErrSQLQuotaKeysExceeded) {
		t.Fatalf("begin bob error = %v, want ErrSQLQuotaKeysExceeded", err)
	}
	now = now.Add(time.Minute)
	if _, err := registry.Begin("bob"); err != nil {
		t.Fatalf("begin bob after window: %v", err)
	}
}

func TestCH029SQLQuotaRegistryUsesBoundedBucketState(t *testing.T) {
	now := time.Unix(300, 0)
	registry, err := NewSQLQuotaRegistry(SQLQuotaRegistryOptions{
		Limits:  SQLQuotaLimits{Window: time.Minute},
		MaxKeys: 1,
		Now:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new bucketed SQL quota registry: %v", err)
	}
	for index := 0; index < 10000; index++ {
		reservation, err := registry.Begin("same")
		if err != nil {
			t.Fatalf("begin quota %d: %v", index, err)
		}
		if err := reservation.Finish(1, time.Nanosecond); err != nil {
			t.Fatalf("finish quota %d: %v", index, err)
		}
	}
	shard := &registry.shards[sqlQuotaShardIndex("same")]
	shard.mu.Lock()
	state := shard.entries["same"]
	buckets := len(state.buckets)
	shard.mu.Unlock()
	if buckets != sqlQuotaBucketCount {
		t.Fatalf("quota bucket count = %d, want %d", buckets, sqlQuotaBucketCount)
	}
}
