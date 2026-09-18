package hatSchema

import (
	"encoding/json"
	"testing"
)

var mz048ManifestBytesSink []byte
var mz048ManifestSink SpaceCatalogManifest

func BenchmarkMZ048ManualJSONMarshal(b *testing.B) {
	manifest := benchmarkMZ048Manifest()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := json.Marshal(manifest)
		if err != nil {
			b.Fatal(err)
		}
		mz048ManifestBytesSink = encoded
	}
}

func BenchmarkMZ048ManagedManifestEncode(b *testing.B) {
	manifest := benchmarkMZ048Manifest()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := encodeSpaceCatalogManifest(manifest)
		if err != nil {
			b.Fatal(err)
		}
		mz048ManifestBytesSink = encoded
	}
}

func BenchmarkMZ048ManualJSONUnmarshal(b *testing.B) {
	encoded, err := json.Marshal(benchmarkMZ048Manifest())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var manifest SpaceCatalogManifest
		if err := json.Unmarshal(encoded, &manifest); err != nil {
			b.Fatal(err)
		}
		mz048ManifestSink = manifest
	}
}

func BenchmarkMZ048ManagedManifestDecode(b *testing.B) {
	encoded, err := encodeSpaceCatalogManifest(benchmarkMZ048Manifest())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		manifest, _, err := decodeSpaceCatalogManifest(encoded)
		if err != nil {
			b.Fatal(err)
		}
		mz048ManifestSink = manifest
	}
}

func benchmarkMZ048Manifest() SpaceCatalogManifest {
	spaces := make([]SpaceDefinition, 64)
	for index := range spaces {
		spaces[index] = SpaceDefinition{Name: "space-" + benchmarkMZ048Integer(index), Version: uint64(index + 1)}
	}
	catalog, err := NewSpaceCatalog(spaces)
	if err != nil {
		panic(err)
	}
	return SpaceCatalogManifest{
		Version:    SpaceCatalogManifestVersion,
		Generation: 27,
		Spaces:     catalog.List(),
	}
}

func benchmarkMZ048Integer(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}
