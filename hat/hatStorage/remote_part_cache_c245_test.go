package hatStorage

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func c245Reference(t *testing.T, name string) RemotePartReference {
	t.Helper()
	reference, err := NewRemotePartReference("s3://bucket/parts/"+name, "parts/"+name+".json", "sha256:"+name, 3)
	if err != nil {
		t.Fatalf("NewRemotePartReference() error = %v", err)
	}
	return reference
}

func TestC245RemotePartCacheAdmissionRequiresRepeatedAccess(t *testing.T) {
	cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: 3, MaxEntries: 2, MinAccesses: 2})
	if err != nil {
		t.Fatalf("NewRemotePartCache() error = %v", err)
	}
	reference := c245Reference(t, "hot")
	var loads atomic.Int32
	loader := func(context.Context, RemotePartReference) ([]byte, error) {
		loads.Add(1)
		return []byte("abc"), nil
	}
	for index := 0; index < 2; index++ {
		data, getErr := cache.Get(context.Background(), reference, 0, loader)
		if getErr != nil || string(data) != "abc" {
			t.Fatalf("Get(%d) = %q, %v", index, data, getErr)
		}
	}
	if got := cache.Stats(); got.Entries != 1 || got.Admissions != 1 || got.Uncached != 1 {
		t.Fatalf("post-admission stats = %+v", got)
	}
	if _, getErr := cache.Get(context.Background(), reference, 0, loader); getErr != nil {
		t.Fatalf("warm Get() error = %v", getErr)
	}
	if got := loads.Load(); got != 2 {
		t.Fatalf("loader calls = %d, want 2", got)
	}
	if got := cache.Stats(); got.Hits != 1 || got.Misses != 2 {
		t.Fatalf("final stats = %+v", got)
	}
}

func TestC245RemotePartCacheDefaultAdmissionRemainsEager(t *testing.T) {
	cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: 3, MaxEntries: 1})
	if err != nil {
		t.Fatalf("NewRemotePartCache() error = %v", err)
	}
	reference := c245Reference(t, "legacy")
	if _, err := cache.Get(context.Background(), reference, 0, func(context.Context, RemotePartReference) ([]byte, error) {
		return []byte("abc"), nil
	}); err != nil {
		t.Fatalf("legacy Get() error = %v", err)
	}
	if got := cache.Stats(); got.Entries != 1 || got.Admissions != 1 {
		t.Fatalf("legacy stats = %+v", got)
	}
}

func TestC245RemotePartCacheConcurrentAccessesPromoteOneLoad(t *testing.T) {
	cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: 3, MaxEntries: 1, MinAccesses: 2})
	if err != nil {
		t.Fatalf("NewRemotePartCache() error = %v", err)
	}
	reference := c245Reference(t, "concurrent")
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var loads atomic.Int32
	loader := func(ctx context.Context, _ RemotePartReference) ([]byte, error) {
		loads.Add(1)
		once.Do(func() { close(started) })
		select {
		case <-release:
			return []byte("abc"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	firstDone := make(chan error, 1)
	go func() {
		_, getErr := cache.Get(context.Background(), reference, 0, loader)
		firstDone <- getErr
	}()
	<-started
	secondDone := make(chan error, 1)
	go func() {
		_, getErr := cache.Get(context.Background(), reference, 0, loader)
		secondDone <- getErr
	}()
	key, err := validateRemotePartCacheReference(reference)
	if err != nil {
		t.Fatalf("validate reference: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		cache.mu.Lock()
		current := cache.loading[key]
		promoted := current != nil && current.admit
		cache.mu.Unlock()
		if promoted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second access did not join the in-flight load")
		}
		runtime.Gosched()
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("single-flight loader calls = %d, want 1", got)
	}
	if _, err := cache.Get(context.Background(), reference, 0, loader); err != nil {
		t.Fatalf("warm Get() error = %v", err)
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("post-promotion loader calls = %d, want 1", got)
	}
}

func TestC245RemotePartCacheAdmissionCandidatesAreBounded(t *testing.T) {
	cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: 3, MaxEntries: 2, MinAccesses: 2})
	if err != nil {
		t.Fatalf("NewRemotePartCache() error = %v", err)
	}
	for index := 0; index < 32; index++ {
		reference := c245Reference(t, "candidate-"+strconv.Itoa(index))
		if _, getErr := cache.Get(context.Background(), reference, 0, func(context.Context, RemotePartReference) ([]byte, error) {
			return []byte("abc"), nil
		}); getErr != nil {
			t.Fatalf("Get(%d) error = %v", index, getErr)
		}
	}
	if got, max := len(cache.candidates), cache.candidateLimit; got > max {
		t.Fatalf("candidate entries = %d, max %d", got, max)
	}
}

func TestC245RemotePartCacheAdmissionAvoidsColdRetention(t *testing.T) {
	const partCount = 32
	loader := func(context.Context, RemotePartReference) ([]byte, error) {
		return []byte("abc"), nil
	}
	eager, err := NewRemotePartCache(RemotePartCacheOptions{
		MaxBytes:   partCount * 3,
		MaxEntries: partCount,
	})
	if err != nil {
		t.Fatalf("NewRemotePartCache(eager) error = %v", err)
	}
	frequency, err := NewRemotePartCache(RemotePartCacheOptions{
		MaxBytes:    partCount * 3,
		MaxEntries:  partCount,
		MinAccesses: 2,
	})
	if err != nil {
		t.Fatalf("NewRemotePartCache(frequency) error = %v", err)
	}
	for index := 0; index < partCount; index++ {
		reference := c245Reference(t, "cold-retention-"+strconv.Itoa(index))
		if _, getErr := eager.Get(context.Background(), reference, 0, loader); getErr != nil {
			t.Fatalf("eager Get(%d) error = %v", index, getErr)
		}
		if _, getErr := frequency.Get(context.Background(), reference, 0, loader); getErr != nil {
			t.Fatalf("frequency Get(%d) error = %v", index, getErr)
		}
	}
	if got := eager.Stats(); got.Entries != partCount || got.Bytes != partCount*3 {
		t.Fatalf("eager retained stats = %+v", got)
	}
	if got := frequency.Stats(); got.Entries != 0 || got.Bytes != 0 || got.Uncached != partCount {
		t.Fatalf("frequency retained stats = %+v", got)
	}
}
