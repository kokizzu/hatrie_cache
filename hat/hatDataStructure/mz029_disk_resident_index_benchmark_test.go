package hatDataStructure_test

import (
	"bytes"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func BenchmarkMZ029DiskResidentReopenSpillableArrangement(b *testing.B) {
	directory := b.TempDir()
	options := hatDataStructure.SpillableArrangementOptions{
		Directory:        directory,
		MemoryLimitBytes: 1,
	}
	arrangement, err := hatDataStructure.NewSpillableArrangement(options)
	if err != nil {
		b.Fatal(err)
	}
	value := bytes.Repeat([]byte{'v'}, 256)
	for index := 0; index < 4096; index++ {
		key := fmt.Sprintf("key-%04d", index)
		if err := arrangement.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
	if err := arrangement.Flush(); err != nil {
		b.Fatal(err)
	}
	spillPath := arrangement.SpillPath()
	if err := arrangement.Close(); err != nil {
		b.Fatal(err)
	}

	diskOptions := options
	diskOptions.DiskResidentIndex = true
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		reopened, err := hatDataStructure.OpenSpillableArrangement(spillPath, diskOptions)
		if err != nil {
			b.Fatal(err)
		}
		if stats := reopened.Stats(); !stats.DiskResidentIndex || stats.IndexResidentEntries != 0 {
			b.Fatalf("reopen stats = %#v, want disk-resident index", stats)
		}
		got, found, err := reopened.Get("key-3072")
		if err != nil {
			b.Fatal(err)
		}
		if !found || len(got) != len(value) {
			b.Fatalf("Get() = (%d bytes, %t), want (%d bytes, true)", len(got), found, len(value))
		}
		if err := reopened.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ029DiskResidentGet(b *testing.B) {
	spillPath, options := prepareMZ029DiskResidentBenchmark(b)
	options.DiskResidentIndex = true
	arrangement, err := hatDataStructure.OpenSpillableArrangement(spillPath, options)
	if err != nil {
		b.Fatal(err)
	}
	defer arrangement.Close()
	if stats := arrangement.Stats(); !stats.DiskResidentIndex {
		b.Fatalf("stats = %#v, want disk-resident index", stats)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum byte
	for index := 0; index < b.N; index++ {
		value, found, err := arrangement.Get("key-3072")
		if err != nil || !found {
			b.Fatalf("Get() = (%d bytes, %t, %v)", len(value), found, err)
		}
		checksum ^= value[index%len(value)]
	}
	b.StopTimer()
	if checksum == 0xff {
		b.Fatal("unexpected checksum")
	}
}

func BenchmarkMZ029MapResidentGet(b *testing.B) {
	spillPath, options := prepareMZ029DiskResidentBenchmark(b)
	arrangement, err := hatDataStructure.OpenSpillableArrangement(spillPath, options)
	if err != nil {
		b.Fatal(err)
	}
	defer arrangement.Close()
	if stats := arrangement.Stats(); stats.DiskResidentIndex || stats.IndexResidentEntries != 4096 {
		b.Fatalf("stats = %#v, want map-resident index", stats)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum byte
	for index := 0; index < b.N; index++ {
		value, found, err := arrangement.Get("key-3072")
		if err != nil || !found {
			b.Fatalf("Get() = (%d bytes, %t, %v)", len(value), found, err)
		}
		checksum ^= value[index%len(value)]
	}
	b.StopTimer()
	if checksum == 0xff {
		b.Fatal("unexpected checksum")
	}
}

func prepareMZ029DiskResidentBenchmark(b *testing.B) (string, hatDataStructure.SpillableArrangementOptions) {
	b.Helper()
	options := hatDataStructure.SpillableArrangementOptions{
		Directory:        b.TempDir(),
		MemoryLimitBytes: 1,
		MaxDiskBytes:     64 << 20,
	}
	arrangement, err := hatDataStructure.NewSpillableArrangement(options)
	if err != nil {
		b.Fatal(err)
	}
	value := bytes.Repeat([]byte{'v'}, 256)
	for index := 0; index < 4096; index++ {
		key := fmt.Sprintf("key-%04d", index)
		if err := arrangement.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
	if err := arrangement.Flush(); err != nil {
		b.Fatal(err)
	}
	spillPath := arrangement.SpillPath()
	if err := arrangement.Close(); err != nil {
		b.Fatal(err)
	}
	return spillPath, options
}
