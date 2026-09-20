package hatDataStructure

import (
	"strconv"
	"testing"
)

var tu22BenchmarkSink uint64

type tu22BenchmarkRow struct {
	Email  string
	Handle string
}

func BenchmarkTU22BaselineIDLookup(b *testing.B) {
	keys := make(map[uint64][2]string, 256)
	for id := uint64(1); id <= 256; id++ {
		keys[id] = [2]string{tu22Email(id), tu22Handle(id)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		id := uint64(index%256) + 1
		value := keys[id]
		tu22BenchmarkSink = uint64(len(value[0]) + len(value[1]))
	}
}

func BenchmarkTU22BaselineAtomicInsert(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		emailOwners := make(map[string]uint64, 256)
		handleOwners := make(map[string]uint64, 256)
		keys := make(map[uint64][2]string, 256)
		for id := uint64(1); id <= 256; id++ {
			row := tu22BenchmarkRow{Email: tu22Email(id), Handle: tu22Handle(id)}
			emailOwners[row.Email] = id
			handleOwners[row.Handle] = id
			keys[id] = [2]string{row.Email, row.Handle}
		}
		tu22BenchmarkSink = uint64(len(emailOwners) + len(handleOwners) + len(keys))
	}
}

func tu22Email(id uint64) string {
	return "user" + strconv.FormatUint(id, 10) + "@example.com"
}

func tu22Handle(id uint64) string {
	return "user" + strconv.FormatUint(id, 10)
}
