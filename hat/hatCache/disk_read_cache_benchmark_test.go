package hatCache

import (
	"bytes"
	"testing"
)

var diskStorageReadBenchmarkSink []byte

func BenchmarkDiskStorageGetReadPath(b *testing.B) {
	for _, test := range []struct {
		name      string
		configure func(*DiskStorage, int32, *testing.B)
	}{
		{name: "filesystem"},
		{
			name: "admitted-cache",
			configure: func(disks *DiskStorage, idx int32, b *testing.B) {
				if err := disks.ConfigureReadCache(DiskStorageReadCacheOptions{
					MaxBytes:       128 << 10,
					MaxValueBytes:  64 << 10,
					AdmissionReads: 1,
				}); err != nil {
					b.Fatalf("ConfigureReadCache() error = %v", err)
				}
				if _, err := disks.Get(idx); err != nil {
					b.Fatalf("warm Get() error = %v", err)
				}
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			disks, err := CreateDiskStorage(b.TempDir(), false)
			if err != nil {
				b.Fatalf("CreateDiskStorage() error = %v", err)
			}
			idx, err := disks.Add(bytes.Repeat([]byte("x"), 64<<10))
			if err != nil {
				b.Fatalf("Add() error = %v", err)
			}
			if test.configure != nil {
				test.configure(disks, idx, b)
			}
			b.SetBytes(64 << 10)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				value, err := disks.Get(idx)
				if err != nil {
					b.Fatalf("Get() error = %v", err)
				}
				diskStorageReadBenchmarkSink = value
			}
		})
	}
}
