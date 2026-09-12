package hatReplication

import "testing"

func BenchmarkChangefeedFrontierProgress(b *testing.B) {
	frontier := NewChangefeedFrontier(100)
	b.ReportAllocs()
	for range b.N {
		progress, err := frontier.Progress()
		if err != nil || !progress.Progressed || progress.Sequence != 100 {
			b.Fatal(err)
		}
	}
}

func BenchmarkChangefeedFrontierAdvance(b *testing.B) {
	frontier := NewChangefeedFrontier(0)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		progress, err := frontier.Advance(uint64(index + 1))
		if err != nil || progress.Sequence != uint64(index+1) {
			b.Fatal(err)
		}
	}
}
