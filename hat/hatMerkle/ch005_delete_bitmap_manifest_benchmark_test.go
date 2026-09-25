package hatMerkle

import "testing"

func BenchmarkCH005PartCatalogWithoutDeleteBitmap(b *testing.B) {
	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := catalog.Attach(PartCatalogEntry{
		Name:     "part-0001",
		Location: "part-0001",
		Manifest: PartManifest{Checksum: ChecksumPart([]byte("immutable-part"))},
	}, func(PartCatalogEntry) error { return nil }); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	encoded, err := catalog.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportMetric(float64(len(encoded)), "checkpoint_bytes/op")
	for index := 0; index < b.N; index++ {
		if _, err := catalog.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH005PartCatalogWithDeleteBitmap(b *testing.B) {
	bitmap, err := BuildPartDeleteBitmap([]byte("delete-bitmap-snapshot"), 128, 7)
	if err != nil {
		b.Fatal(err)
	}
	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := catalog.Attach(PartCatalogEntry{
		Name:     "part-0001",
		Location: "part-0001",
		Manifest: PartManifest{Checksum: ChecksumPart([]byte("immutable-part")), DeleteBitmap: &bitmap},
	}, func(PartCatalogEntry) error { return nil }); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	encoded, err := catalog.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportMetric(float64(len(encoded)), "checkpoint_bytes/op")
	for index := 0; index < b.N; index++ {
		if _, err := catalog.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}
