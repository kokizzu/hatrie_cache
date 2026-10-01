package hatExtension

import "testing"

func BenchmarkNativeExtensionRegistryResolve(b *testing.B) {
	registry := NewRegistry()
	if _, err := registry.Register(testNativeExtension{manifest: testManifest()}, ""); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := registry.Resolve("geo"); !ok {
			b.Fatal("missing extension")
		}
	}
}

func BenchmarkNativeExtensionRegistryMetadata(b *testing.B) {
	registry := NewRegistry()
	if _, err := registry.Register(testNativeExtension{manifest: testManifest()}, ""); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := registry.Metadata("geo"); !ok {
			b.Fatal("missing extension metadata")
		}
	}
}

func BenchmarkNativeExtensionManifestNormalize(b *testing.B) {
	manifest := testManifest()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manifest.Normalize(); err != nil {
			b.Fatal(err)
		}
	}
}
