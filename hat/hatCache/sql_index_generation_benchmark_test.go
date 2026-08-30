package hatCache

import (
	"strings"
	"testing"
)

var benchmarkSQLIndexFreshnessCurrent bool

func BenchmarkSQLIndexFreshness(b *testing.B) {
	raw := strings.Repeat(`[{"id":1,"name":"Ada Lovelace"}]`, 32_768)
	equivalent := string(append([]byte(nil), raw...))
	state := sqlJSONIndexState{raw: raw, generation: 7, ready: true}
	b.ReportAllocs()
	b.Run("raw_string_equality", func(b *testing.B) {
		source := sqlJSONSource{raw: equivalent}
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkSQLIndexFreshnessCurrent = source.current(state)
		}
	})
	b.Run("write_generation", func(b *testing.B) {
		source := sqlJSONSource{raw: raw, generation: 7, tracked: true}
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkSQLIndexFreshnessCurrent = source.current(state)
		}
	})
}
