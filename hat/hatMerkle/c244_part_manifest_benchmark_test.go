package hatMerkle

import "testing"

func BenchmarkC244PartManifestEqual(b *testing.B) {
	data, ranges := c244BenchmarkPart()
	manifest, err := BuildPartManifest(data, ranges)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !manifest.Equal(manifest) {
			b.Fatal("manifest did not equal itself")
		}
	}
}

func BenchmarkC244PartManifestValidate(b *testing.B) {
	data, ranges := c244BenchmarkPart()
	manifest, err := BuildPartManifest(data, ranges)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := manifest.Validate(data); err != nil {
			b.Fatal(err)
		}
	}
}

func c244BenchmarkPart() ([]byte, []PartColumnRange) {
	data := make([]byte, 1<<20)
	for index := range data {
		data[index] = byte(index)
	}
	columnSize := uint64(len(data) / 4)
	return data, []PartColumnRange{
		{Name: "column-0", Offset: 0, Size: columnSize},
		{Name: "column-1", Offset: columnSize, Size: columnSize},
		{Name: "column-2", Offset: columnSize * 2, Size: columnSize},
		{Name: "column-3", Offset: columnSize * 3, Size: columnSize},
	}
}
