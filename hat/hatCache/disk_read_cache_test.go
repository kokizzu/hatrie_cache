package hatCache

import (
	"bytes"
	"testing"
)

func TestDiskStorageReadCacheAdmitsRepeatedReadsAndInvalidatesWrites(t *testing.T) {
	disks, err := CreateDiskStorage(t.TempDir(), false)
	if err != nil {
		t.Fatalf("CreateDiskStorage() error = %v", err)
	}
	original := []byte("original payload")
	idx, err := disks.Add(original)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if got := disks.ReadCacheStats(); got.Enabled {
		t.Fatalf("default read cache stats = %#v, want disabled", got)
	}
	if err := disks.ConfigureReadCache(DiskStorageReadCacheOptions{
		MaxBytes:       64,
		MaxValueBytes:  64,
		AdmissionReads: 2,
	}); err != nil {
		t.Fatalf("ConfigureReadCache() error = %v", err)
	}

	first, err := disks.Get(idx)
	if err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	if !bytes.Equal(first, original) {
		t.Fatalf("first Get() = %q, want %q", first, original)
	}
	stats := disks.ReadCacheStats()
	if stats.Hits != 0 || stats.Misses != 1 || stats.Entries != 0 {
		t.Fatalf("after first read stats = %#v, want one miss and no entry", stats)
	}

	second, err := disks.Get(idx)
	if err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	second[0] = 'X'
	stats = disks.ReadCacheStats()
	if stats.Admissions != 1 || stats.Entries != 1 || stats.Bytes != int64(len(original)) {
		t.Fatalf("after admission stats = %#v", stats)
	}

	third, err := disks.Get(idx)
	if err != nil {
		t.Fatalf("third Get() error = %v", err)
	}
	if !bytes.Equal(third, original) {
		t.Fatalf("cached Get() = %q, want independent %q", third, original)
	}
	if stats = disks.ReadCacheStats(); stats.Hits != 1 {
		t.Fatalf("after cached read stats = %#v, want one hit", stats)
	}

	updated := []byte("updated payload")
	if err := disks.Put(idx, updated); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if stats = disks.ReadCacheStats(); stats.Entries != 0 || stats.Bytes != 0 {
		t.Fatalf("after Put() stats = %#v, want invalidated cache", stats)
	}
	got, err := disks.Get(idx)
	if err != nil {
		t.Fatalf("updated Get() error = %v", err)
	}
	if !bytes.Equal(got, updated) {
		t.Fatalf("updated Get() = %q, want %q", got, updated)
	}
}

func TestDiskStorageReadCacheRespectsByteBounds(t *testing.T) {
	disks, err := CreateDiskStorage(t.TempDir(), false)
	if err != nil {
		t.Fatalf("CreateDiskStorage() error = %v", err)
	}
	idx, err := disks.Add(bytes.Repeat([]byte("x"), 32))
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := disks.ConfigureReadCache(DiskStorageReadCacheOptions{
		MaxBytes:       16,
		MaxValueBytes:  16,
		AdmissionReads: 1,
	}); err != nil {
		t.Fatalf("ConfigureReadCache() error = %v", err)
	}
	if _, err := disks.Get(idx); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := disks.Get(idx); err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	stats := disks.ReadCacheStats()
	if stats.Entries != 0 || stats.Bytes != 0 || stats.Admissions != 0 {
		t.Fatalf("oversized value stats = %#v, want no admission", stats)
	}
}

func TestDiskStorageReadCacheEvictsLeastRecentlyUsedAndCanDisable(t *testing.T) {
	disks, err := CreateDiskStorage(t.TempDir(), false)
	if err != nil {
		t.Fatalf("CreateDiskStorage() error = %v", err)
	}
	indexes := make([]int32, 3)
	for index := range indexes {
		indexes[index], err = disks.Add(bytes.Repeat([]byte{byte('a' + index)}, 8))
		if err != nil {
			t.Fatalf("Add(%d) error = %v", index, err)
		}
	}
	if err := disks.ConfigureReadCache(DiskStorageReadCacheOptions{
		MaxBytes:       16,
		MaxValueBytes:  8,
		AdmissionReads: 1,
	}); err != nil {
		t.Fatalf("ConfigureReadCache() error = %v", err)
	}
	for _, index := range indexes[:2] {
		if _, err := disks.Get(index); err != nil {
			t.Fatalf("warm Get(%d) error = %v", index, err)
		}
	}
	if _, err := disks.Get(indexes[0]); err != nil {
		t.Fatalf("recent Get(0) error = %v", err)
	}
	if _, err := disks.Get(indexes[2]); err != nil {
		t.Fatalf("evicting Get(2) error = %v", err)
	}
	stats := disks.ReadCacheStats()
	if stats.Entries != 2 || stats.Bytes != 16 || stats.Evictions != 1 {
		t.Fatalf("bounded LRU stats = %#v, want two entries, 16 bytes, one eviction", stats)
	}
	misses := stats.Misses
	if _, err := disks.Get(indexes[1]); err != nil {
		t.Fatalf("evicted Get(1) error = %v", err)
	}
	if stats = disks.ReadCacheStats(); stats.Misses != misses+1 {
		t.Fatalf("evicted Get() stats = %#v, want one additional miss", stats)
	}
	if err := disks.ConfigureReadCache(DiskStorageReadCacheOptions{}); err != nil {
		t.Fatalf("disable ConfigureReadCache() error = %v", err)
	}
	if got := disks.ReadCacheStats(); got.Enabled {
		t.Fatalf("disabled read cache stats = %#v, want disabled", got)
	}
}

func TestDiskStorageReadCacheRejectsNegativeBounds(t *testing.T) {
	disks, err := CreateDiskStorage(t.TempDir(), false)
	if err != nil {
		t.Fatalf("CreateDiskStorage() error = %v", err)
	}
	if err := disks.ConfigureReadCache(DiskStorageReadCacheOptions{MaxBytes: -1}); err != ErrDiskStorageReadCacheInvalidMaxBytes {
		t.Fatalf("negative MaxBytes error = %v, want %v", err, ErrDiskStorageReadCacheInvalidMaxBytes)
	}
	if err := disks.ConfigureReadCache(DiskStorageReadCacheOptions{MaxValueBytes: -1}); err != ErrDiskStorageReadCacheInvalidMaxValue {
		t.Fatalf("negative MaxValueBytes error = %v, want %v", err, ErrDiskStorageReadCacheInvalidMaxValue)
	}
}

func TestHatTrieDiskReadCacheIsOptIn(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if got := trie.DiskReadCacheStats(); got.Enabled {
		t.Fatalf("default trie read cache stats = %#v, want disabled", got)
	}
	if err := trie.ConfigureDiskReadCache(DiskStorageReadCacheOptions{MaxBytes: 128}); err != nil {
		t.Fatalf("ConfigureDiskReadCache() error = %v", err)
	}
	if got := trie.DiskReadCacheStats(); !got.Enabled || got.MaxBytes != 128 {
		t.Fatalf("configured trie read cache stats = %#v", got)
	}
}
