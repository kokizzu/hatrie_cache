package hatStorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestCH021TieredPartReaderReadsLocalTier(t *testing.T) {
	root := t.TempDir()
	want := []byte("local-tier-part")
	path := filepath.Join(root, "parts", "part.bin")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := NewStorageTierReader(StorageTierReaderOptions{LocalRoot: root})
	if err != nil {
		t.Fatalf("NewStorageTierReader() error = %v", err)
	}
	got, err := reader.Read(context.Background(), StorageTierReadPart{
		Key:       "part-1",
		Tier:      StorageTierLocal,
		LocalPath: "parts/part.bin",
	})
	if err != nil {
		t.Fatalf("Read(local) error = %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("Read(local) = %q, want %q", got, want)
	}
}

func TestCH021TieredPartReaderReadsRemoteThroughCache(t *testing.T) {
	want := []byte("object-tier-part")
	digest := sha256.Sum256(want)
	reference, err := NewRemotePartReference(
		"https://objects.example.test/parts/part.bin",
		"parts/part.bin",
		hex.EncodeToString(digest[:]),
		uint64(len(want)),
	)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: 1024, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	var loads atomic.Int32
	reader, err := NewStorageTierReader(StorageTierReaderOptions{
		RemoteCache: cache,
		RemoteLoader: func(context.Context, RemotePartReference) ([]byte, error) {
			loads.Add(1)
			return append([]byte(nil), want...), nil
		},
	})
	if err != nil {
		t.Fatalf("NewStorageTierReader() error = %v", err)
	}
	part := StorageTierReadPart{Key: "part-1", Tier: StorageTierObject, Remote: reference}
	for attempt := 0; attempt < 2; attempt++ {
		got, readErr := reader.Read(context.Background(), part)
		if readErr != nil {
			t.Fatalf("Read(remote) attempt %d error = %v", attempt, readErr)
		}
		if string(got) != string(want) {
			t.Fatalf("Read(remote) attempt %d = %q, want %q", attempt, got, want)
		}
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("remote loader calls = %d, want one cache fill", got)
	}
}

func TestCH021TieredPartReaderRejectsUnsafeOrIncompleteReads(t *testing.T) {
	reader, err := NewStorageTierReader(StorageTierReaderOptions{LocalRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(context.Background(), StorageTierReadPart{
		Key:       "unsafe",
		Tier:      StorageTierLocal,
		LocalPath: "../outside",
	}); !errors.Is(err, ErrStorageTierReaderInvalid) {
		t.Fatalf("unsafe local path error = %v, want ErrStorageTierReaderInvalid", err)
	}
	if _, err := reader.Read(context.Background(), StorageTierReadPart{
		Key:  "missing-remote",
		Tier: StorageTierObject,
	}); !errors.Is(err, ErrStorageTierReaderRemoteCacheNeeded) {
		t.Fatalf("missing remote cache error = %v, want ErrStorageTierReaderRemoteCacheNeeded", err)
	}
	cache, err := NewRemotePartCache(RemotePartCacheOptions{MaxBytes: 64, MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	remoteReader, err := NewStorageTierReader(StorageTierReaderOptions{
		RemoteCache:  cache,
		RemoteLoader: func(context.Context, RemotePartReference) ([]byte, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remoteReader.Read(context.Background(), StorageTierReadPart{
		Key:  "missing-reference",
		Tier: StorageTierObject,
	}); !errors.Is(err, ErrStorageTierReaderInvalid) {
		t.Fatalf("missing remote reference error = %v, want ErrStorageTierReaderInvalid", err)
	}
	if _, err := reader.Read(nil, StorageTierReadPart{Key: "local", Tier: StorageTierLocal, LocalPath: "x"}); !errors.Is(err, ErrStorageTierReaderContextRequired) {
		t.Fatalf("nil context error = %v, want ErrStorageTierReaderContextRequired", err)
	}
}
