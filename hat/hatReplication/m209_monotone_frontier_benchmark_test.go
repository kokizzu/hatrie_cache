package hatReplication_test

import (
	"testing"

	"hatrie_cache/hat/hatReplication"
)

var m209ChangefeedFrontierAdvanceSink uint64

func BenchmarkM209ChangefeedFrontierAdvance(b *testing.B) {
	frontier := hatReplication.NewChangefeedFrontier(0)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := frontier.Advance(uint64(index + 1)); err != nil {
			b.Fatal(err)
		}
	}
	m209ChangefeedFrontierAdvanceSink = frontier.Current()
}
