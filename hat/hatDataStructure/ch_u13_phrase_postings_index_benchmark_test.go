package hatDataStructure

import (
	"runtime"
	"testing"
)

func buildCHU13PhraseIndex(documents []chU13BenchmarkDocument) *TokenPostingsIndex {
	index := NewTokenPostingsIndexWithPhrases()
	for _, document := range documents {
		index.Upsert(document.row, document.text)
	}
	return index
}

func BenchmarkCHU13PhraseMatchAll(b *testing.B) {
	index := buildCHU13PhraseIndex(chU13BenchmarkDocuments())
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_ = index.MatchAll("quick brown fox")
	}
}

func BenchmarkCHU13PhraseMatchPhrase(b *testing.B) {
	index := buildCHU13PhraseIndex(chU13BenchmarkDocuments())
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_ = index.MatchPhrase("quick brown fox")
	}
}

func BenchmarkCHU13PhraseUpsert(b *testing.B) {
	index := NewTokenPostingsIndexWithPhrases()
	texts := [2]string{
		"quick brown fox jumps over the lazy dog",
		"quick red fox jumps over the lazy dog",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		index.Upsert(1, texts[iteration&1])
	}
}

func BenchmarkCHU13PhraseMemory(b *testing.B) {
	documents := chU13BenchmarkDocuments()
	base := buildCHU13BaseIndex(documents)
	phrase := buildCHU13PhraseIndex(documents)
	baseInfo := base.Info()
	phraseInfo := phrase.PhraseInfo()
	b.ReportMetric(float64(baseInfo.EncodedBytes), "base_roaring_payload_bytes")
	b.ReportMetric(float64(phraseInfo.SequenceBytes), "phrase_sequence_bytes")
	b.ReportMetric(float64(phraseInfo.EncodedBytes), "phrase_roaring_payload_bytes")
	b.ReportMetric(float64(phraseInfo.SequenceBytes+phraseInfo.EncodedBytes), "phrase_tracked_payload_bytes")
	for iteration := 0; iteration < b.N; iteration++ {
		runtime.KeepAlive(base)
		runtime.KeepAlive(phrase)
	}
}

func TestCHU13PhrasePostingsMemoryReport(t *testing.T) {
	documents := chU13BenchmarkDocuments()
	baseInfo := buildCHU13BaseIndex(documents).Info()
	phraseInfo := buildCHU13PhraseIndex(documents).PhraseInfo()
	t.Logf("rows=%d base_roaring_payload_bytes=%d phrase_rows=%d phrase_tokens=%d phrase_bigrams=%d phrase_sequence_bytes=%d phrase_roaring_payload_bytes=%d phrase_tracked_payload_bytes=%d", baseInfo.Rows, baseInfo.EncodedBytes, phraseInfo.Rows, phraseInfo.Tokens, phraseInfo.Bigrams, phraseInfo.SequenceBytes, phraseInfo.EncodedBytes, phraseInfo.SequenceBytes+phraseInfo.EncodedBytes)
}
