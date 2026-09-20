package hatDataStructure

import (
	"runtime"
	"testing"
	"time"
)

type verticalTTLFullRow struct {
	key     string
	expires int64
	deleted bool
	payload [512]byte
}

var verticalTTLDeleteBenchmarkSink byte

func BenchmarkPersistentDeleteBitmapVerticalTTL(b *testing.B) {
	const rows = 65_536
	now := time.Unix(100, 0)
	bitmap, keys, expires := verticalTTLDeleteFixture(rows, now)
	b.SetBytes(int64(rows * 8))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := bitmap.ApplyVerticalTTLDeletes(keys, expires, now, 0)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		for _, candidate := range result.Candidates {
			if _, err := bitmap.Undelete(candidate.Row); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
	}
}

func BenchmarkPersistentDeleteBitmapVerticalTTLInto(b *testing.B) {
	const rows = 65_536
	now := time.Unix(100, 0)
	bitmap, keys, expires := verticalTTLDeleteFixture(rows, now)
	candidates := make([]VerticalTTLDeleteCandidate, 0, rows/10)
	b.SetBytes(int64(rows * 8))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := bitmap.ApplyVerticalTTLDeletesInto(candidates, keys, expires, now, 0)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		for _, candidate := range result.Candidates {
			if _, err := bitmap.Undelete(candidate.Row); err != nil {
				b.Fatal(err)
			}
		}
		candidates = result.Candidates[:0]
		b.StartTimer()
	}
}

func BenchmarkPersistentDeleteBitmapFullRowControl(b *testing.B) {
	const rows = 65_536
	now := time.Unix(100, 0)
	fullRows := make([]verticalTTLFullRow, rows)
	for index := range fullRows {
		fullRows[index].key = "key-" + string(rune(index))
		if index%10 == 0 {
			fullRows[index].expires = now.Add(-time.Minute).UnixNano()
		}
		fullRows[index].deleted = index%17 == 0
		fullRows[index].payload[0] = byte(index)
	}
	b.SetBytes(int64(rows * (8 + 512)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		candidates := make([]VerticalTTLDeleteCandidate, 0, rows/10)
		var checksum byte
		for row, value := range fullRows {
			if value.deleted || value.expires == 0 || value.expires > now.UnixNano() {
				continue
			}
			checksum ^= value.payload[0]
			candidates = append(candidates, VerticalTTLDeleteCandidate{Row: uint64(row), Key: value.key})
		}
		verticalTTLDeleteBenchmarkSink ^= checksum
		runtime.KeepAlive(candidates)
	}
}

func verticalTTLDeleteFixture(rows int, now time.Time) (*PersistentDeleteBitmap, []string, []int64) {
	bitmap, err := NewPersistentDeleteBitmap(uint64(rows))
	if err != nil {
		panic(err)
	}
	keys := make([]string, rows)
	expires := make([]int64, rows)
	for index := range keys {
		keys[index] = "key-" + string(rune(index))
		if index%10 == 0 {
			expires[index] = now.Add(-time.Minute).UnixNano()
		}
		if index%17 == 0 {
			if _, err := bitmap.Delete(uint64(index)); err != nil {
				panic(err)
			}
		}
	}
	return bitmap, keys, expires
}
