package hatDataStructure

import "testing"

type tt021BaselineBox struct {
	minX, minY float64
	maxX, maxY float64
}

type tt021BaselineEntry struct {
	tt021BaselineBox
	value int
}

var tt021BaselineSink int

func BenchmarkTT021PackedRTreeLinearScan(b *testing.B) {
	entries := tt021BaselineEntries()
	query := tt021BaselineBox{minX: 50, minY: 50, maxX: 59, maxY: 59}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		matches := 0
		for _, entry := range entries {
			if entry.maxX >= query.minX && entry.minX <= query.maxX && entry.maxY >= query.minY && entry.minY <= query.maxY {
				matches += entry.value
			}
		}
		tt021BaselineSink = matches
	}
}

func tt021BaselineEntries() []tt021BaselineEntry {
	entries := make([]tt021BaselineEntry, 0, 10000)
	for y := 0; y < 100; y++ {
		for x := 0; x < 100; x++ {
			entries = append(entries, tt021BaselineEntry{
				tt021BaselineBox: tt021BaselineBox{
					minX: float64(x),
					minY: float64(y),
					maxX: float64(x) + 0.75,
					maxY: float64(y) + 0.75,
				},
				value: y*100 + x,
			})
		}
	}
	return entries
}
