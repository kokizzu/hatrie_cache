package hatMerkle

import (
	"strconv"
	"testing"
)

func mu38BenchmarkCatalog(b testing.TB, parts int) (*PartCatalog, []byte) {
	b.Helper()
	payload := []byte("immutable benchmark part payload with enough bytes to exercise checksum metadata")
	manifest, err := BuildPartManifest(payload, []PartColumnRange{
		{Name: "key", Offset: 0, Size: 8},
		{Name: "value", Offset: 8, Size: uint64(len(payload) - 8)},
	})
	if err != nil {
		b.Fatal(err)
	}
	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: parts + 1})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < parts; index++ {
		name := "part-" + strconv.Itoa(index)
		if err := catalog.Attach(PartCatalogEntry{
			Name:     name,
			Location: "parts/" + name,
			Manifest: manifest,
		}, func(entry PartCatalogEntry) error {
			return entry.Manifest.Validate(payload)
		}); err != nil {
			b.Fatal(err)
		}
		if index%4 == 0 {
			if _, err := catalog.Detach(name); err != nil {
				b.Fatal(err)
			}
		}
	}
	data, err := catalog.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	return catalog, data
}

func BenchmarkMU38PartCatalogSnapshot(b *testing.B) {
	catalog, _ := mu38BenchmarkCatalog(b, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		active, quarantined := catalog.Snapshot()
		if len(active)+len(quarantined) == 0 {
			b.Fatal("benchmark catalog unexpectedly empty")
		}
	}
}

func BenchmarkMU38PartCatalogMarshalBinary(b *testing.B) {
	catalog, data := mu38BenchmarkCatalog(b, 256)
	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportMetric(float64(len(data)), "checkpoint-bytes")
	for index := 0; index < b.N; index++ {
		encoded, err := catalog.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		if len(encoded) != len(data) {
			b.Fatalf("encoded size = %d, want %d", len(encoded), len(data))
		}
	}
}

func BenchmarkMU38PartCatalogRestoreBinary(b *testing.B) {
	_, data := mu38BenchmarkCatalog(b, 256)
	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportMetric(float64(len(data)), "checkpoint-bytes")
	for index := 0; index < b.N; index++ {
		catalog, err := RestorePartCatalog(data, PartCatalogOptions{MaxEntries: 257})
		if err != nil {
			b.Fatal(err)
		}
		if catalog.Len() == 0 {
			b.Fatal("restored benchmark catalog unexpectedly empty")
		}
	}
}

func BenchmarkMU38PartCatalogLoad(b *testing.B) {
	_, data := mu38BenchmarkCatalog(b, 256)
	path := b.TempDir() + "/catalog.bin"
	fileCatalog, err := RestorePartCatalog(data, PartCatalogOptions{MaxEntries: 257})
	if err != nil {
		b.Fatal(err)
	}
	if err := fileCatalog.Save(path); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportMetric(float64(len(data)), "checkpoint-bytes")
	for index := 0; index < b.N; index++ {
		catalog, err := LoadPartCatalog(path, PartCatalogOptions{MaxEntries: 257})
		if err != nil {
			b.Fatal(err)
		}
		if catalog.Len() == 0 {
			b.Fatal("loaded benchmark catalog unexpectedly empty")
		}
	}
}

func BenchmarkMU38PartCatalogSave(b *testing.B) {
	catalog, data := mu38BenchmarkCatalog(b, 256)
	path := b.TempDir() + "/catalog.bin"
	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportMetric(float64(len(data)), "checkpoint-bytes")
	for index := 0; index < b.N; index++ {
		if err := catalog.Save(path); err != nil {
			b.Fatal(err)
		}
	}
}
