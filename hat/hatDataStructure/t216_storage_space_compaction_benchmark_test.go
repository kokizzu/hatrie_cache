//go:build t216

package hatDataStructure

import (
	"bytes"
	"fmt"
	"testing"
)

func newT216BenchmarkSpace(b *testing.B, mode StorageSpaceMode) *StorageSpace {
	b.Helper()
	options := StorageSpaceOptions{Name: "t216-benchmark", Mode: mode}
	if mode == StorageSpaceOnDisk {
		options.Directory = b.TempDir()
		options.MemoryLimitBytes = 1
	}
	space, err := NewStorageSpace(options)
	if err != nil {
		b.Fatal(err)
	}
	value := bytes.Repeat([]byte("v"), 64)
	for index := 0; index < 256; index++ {
		if err := space.Set(fmt.Sprintf("key-%04d", index), value); err != nil {
			b.Fatal(err)
		}
	}
	if err := space.Flush(); err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if err := space.Set(fmt.Sprintf("key-%04d", index), value); err != nil {
			b.Fatal(err)
		}
	}
	if err := space.Flush(); err != nil {
		b.Fatal(err)
	}
	return space
}

func BenchmarkT216StorageSpaceStats(b *testing.B) {
	space := newT216BenchmarkSpace(b, StorageSpaceOnDisk)
	defer space.Close()
	b.ResetTimer()
	for range b.N {
		_ = space.Stats()
	}
}

func BenchmarkT216StorageSpaceMemtxStats(b *testing.B) {
	space := newT216BenchmarkSpace(b, StorageSpaceMemtx)
	defer space.Close()
	b.ResetTimer()
	for range b.N {
		_ = space.Stats()
	}
}

func BenchmarkT216StorageSpaceCompact(b *testing.B) {
	space := newT216BenchmarkSpace(b, StorageSpaceOnDisk)
	defer space.Close()
	value := bytes.Repeat([]byte("w"), 64)
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		if err := space.Set("key-0000", value); err != nil {
			b.Fatal(err)
		}
		if err := space.Flush(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := space.Compact(); err != nil {
			b.Fatal(err)
		}
	}
}
