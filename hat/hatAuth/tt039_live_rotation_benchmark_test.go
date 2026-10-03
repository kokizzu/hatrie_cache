package hatAuth

import (
	"testing"
	"time"
)

var benchmarkTT039MatchSink bool

func BenchmarkTT039ImmutableTokenSetMatch(b *testing.B) {
	tokens := NewTokenSet("current-token", "previous-token", time.Now().Add(time.Hour))
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkTT039MatchSink = tokens.Matches("current-token", now)
	}
}

func BenchmarkTT039RotatingTokenMatch(b *testing.B) {
	rotator := NewTokenRotator("current-token", "previous-token", time.Now().Add(time.Hour))
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkTT039MatchSink = rotator.Matches("current-token", now)
	}
}

func BenchmarkTT039RotatingTokenRotate(b *testing.B) {
	rotator := NewTokenRotator("current-token", "", time.Time{})
	b.ReportAllocs()
	for range b.N {
		if err := rotator.Rotate("next-token", "current-token", time.Now().Add(time.Hour)); err != nil {
			b.Fatal(err)
		}
	}
}
