package hatBackup

import (
	"path/filepath"
	"testing"
)

func BenchmarkBackupManifestCatalogAppend(b *testing.B) {
	path := filepath.Join(b.TempDir(), "catalog.json")
	catalog, err := NewBackupManifestCatalog(path)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		manifest := catalogTestManifest("backup-"+benchmarkCatalogInteger(index), "", false, uint64(index+1), "object-"+benchmarkCatalogInteger(index))
		if err := catalog.Append(manifest); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBackupManifestCatalogLoad(b *testing.B) {
	path := filepath.Join(b.TempDir(), "catalog.json")
	catalog, err := NewBackupManifestCatalog(path)
	if err != nil {
		b.Fatal(err)
	}
	manifests := make([]BundleManifest, 32)
	for index := range manifests {
		parent := ""
		if index > 0 {
			parent = "backup-" + benchmarkCatalogInteger(index-1)
		}
		manifests[index] = catalogTestManifest("backup-"+benchmarkCatalogInteger(index), parent, index > 0, uint64(index+1), "object-"+benchmarkCatalogInteger(index))
	}
	if err := catalog.Replace(manifests); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		loaded, err := catalog.Load()
		if err != nil {
			b.Fatal(err)
		}
		backupManifestCatalogBenchmarkSink = len(loaded)
	}
}

var backupManifestCatalogBenchmarkSink int

func benchmarkCatalogInteger(value int) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	encoded := make([]byte, 0, 12)
	for value > 0 {
		encoded = append(encoded, digits[value%10])
		value /= 10
	}
	for left, right := 0, len(encoded)-1; left < right; left, right = left+1, right-1 {
		encoded[left], encoded[right] = encoded[right], encoded[left]
	}
	return string(encoded)
}
