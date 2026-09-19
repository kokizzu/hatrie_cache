package hatBackup

import "testing"

var m050BaselineSink uint64

func BenchmarkM050BaselineManifestLoop(b *testing.B) {
	var value uint64
	for index := 0; index < b.N; index++ {
		value += uint64(index) ^ 0x9e3779b97f4a7c15
	}
	m050BaselineSink = value
}
