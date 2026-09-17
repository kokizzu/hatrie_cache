package hatSql

import (
	"bytes"
	"errors"
	"strconv"
	"sync"
	"testing"
)

func TestSQLSpillQuotaSharedByWriters(t *testing.T) {
	quota := newSQLSpillQuota(5)
	firstAvailable, secondAvailable := int64(100), int64(100)
	first := sqlSpillBudgetWriter{writer: &bytes.Buffer{}, available: &firstAvailable, quota: quota, path: "first"}
	second := sqlSpillBudgetWriter{writer: &bytes.Buffer{}, available: &secondAvailable, quota: quota, path: "second"}
	if written, err := first.Write([]byte("1234")); err != nil || written != 4 {
		t.Fatalf("first write = (%d, %v), want (4, nil)", written, err)
	}
	if _, err := second.Write([]byte("12")); !errors.Is(err, errSQLQuerySpillDiskBudget) {
		t.Fatalf("second write error = %v, want query spill budget error", err)
	}
	quota.release("first")
	if written, err := second.Write([]byte("12")); err != nil || written != 2 {
		t.Fatalf("second write after release = (%d, %v), want (2, nil)", written, err)
	}
	quota.release("second")
	if got := quota.usedBytes(); got != 0 {
		t.Fatalf("used bytes after shared writers = %d, want 0", got)
	}
}

func TestSQLSpillQuotaTracksLiveBytesAndReleasesPath(t *testing.T) {
	quota := newSQLSpillQuota(10)
	if !quota.reserve("first", 6) {
		t.Fatal("first reservation failed")
	}
	if quota.reserve("second", 5) {
		t.Fatal("second reservation succeeded past the limit")
	}
	quota.release("first")
	if !quota.reserve("second", 5) {
		t.Fatal("second reservation failed after releasing the first path")
	}
	quota.release("second")
	if got := quota.usedBytes(); got != 0 {
		t.Fatalf("used bytes after releasing all paths = %d, want 0", got)
	}
}

func TestSQLSpillQuotaConcurrentReservationsStayBounded(t *testing.T) {
	quota := newSQLSpillQuota(32)
	var wait sync.WaitGroup
	for index := 0; index < 128; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			path := "path-" + strconv.Itoa(index)
			if quota.reserve(path, 1) {
				quota.release(path)
			}
		}(index)
	}
	wait.Wait()
	if got := quota.usedBytes(); got < 0 || got > 32 {
		t.Fatalf("concurrent used bytes = %d, want between 0 and 32", got)
	}
}
