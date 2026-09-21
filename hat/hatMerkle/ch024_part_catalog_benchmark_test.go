package hatMerkle

import (
	"io"
	"os"
	"testing"
)

type ch024BaselinePart struct {
	location string
}

func BenchmarkCH024PartLifecycleBaseline(b *testing.B) {
	active := make(map[string]ch024BaselinePart, 1)
	quarantined := make(map[string]ch024BaselinePart, 1)
	old := ch024BaselinePart{location: "part-0001.old"}
	replacement := ch024BaselinePart{location: "part-0001.new"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		active["part-0001"] = old
		quarantined["part-0001"] = active["part-0001"]
		delete(active, "part-0001")
		active["part-0001"] = replacement
		delete(quarantined, "part-0001")
		quarantined["part-0001"] = active["part-0001"]
		delete(active, "part-0001")
		delete(quarantined, "part-0001")
	}
}

func BenchmarkCH024PartVerificationReadAll(b *testing.B) {
	payload := make([]byte, 1<<20)
	for index := range payload {
		payload[index] = byte(index)
	}
	file, err := os.CreateTemp(b.TempDir(), "part-*")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := file.Write(payload); err != nil {
		b.Fatal(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		b.Fatal(err)
	}
	checksum := ChecksumPart(payload)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		data, err := io.ReadAll(file)
		if err != nil {
			b.Fatal(err)
		}
		if !VerifyPartChecksum(data, checksum) {
			b.Fatal("read-all verification failed")
		}
	}
}

func BenchmarkCH024PartVerificationStreaming(b *testing.B) {
	payload := make([]byte, 1<<20)
	for index := range payload {
		payload[index] = byte(index)
	}
	file, err := os.CreateTemp(b.TempDir(), "part-*")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := file.Write(payload); err != nil {
		b.Fatal(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		b.Fatal(err)
	}
	checksum := ChecksumPart(payload)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := VerifyImmutablePartFile(file, checksum); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH024PartCatalogLifecycle(b *testing.B) {
	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 4})
	if err != nil {
		b.Fatal(err)
	}
	verify := func(PartCatalogEntry) error { return nil }
	old := PartCatalogEntry{Name: "part-0001", Location: "part-0001.old"}
	replacement := PartCatalogEntry{Name: "part-0001", Location: "part-0001.new"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := catalog.Attach(old, verify); err != nil {
			b.Fatal(err)
		}
		if _, err := catalog.Detach(old.Name); err != nil {
			b.Fatal(err)
		}
		if err := catalog.Attach(replacement, verify); err != nil {
			b.Fatal(err)
		}
		if !catalog.RemoveQuarantined(old.Name) {
			b.Fatal("quarantine removal failed")
		}
		if _, err := catalog.Detach(replacement.Name); err != nil {
			b.Fatal(err)
		}
		if !catalog.RemoveQuarantined(replacement.Name) {
			b.Fatal("replacement quarantine removal failed")
		}
	}
}
