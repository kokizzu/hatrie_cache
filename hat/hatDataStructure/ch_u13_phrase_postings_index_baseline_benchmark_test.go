package hatDataStructure

import "testing"

type chU13BenchmarkDocument struct {
	row  uint32
	text string
}

func chU13BenchmarkDocuments() []chU13BenchmarkDocument {
	documents := make([]chU13BenchmarkDocument, 20_000)
	for row := range documents {
		var text string
		switch row % 5 {
		case 0:
			text = "quick brown fox jumps over the lazy dog"
		case 1:
			text = "quick red fox jumps over the lazy dog"
		case 2:
			text = "the quick brown fox jumps over the lazy dog"
		case 3:
			text = "brown quick fox jumps over the lazy dog"
		default:
			text = "quick brown brown fox jumps over the lazy dog"
		}
		documents[row] = chU13BenchmarkDocument{row: uint32(row + 1), text: text}
	}
	return documents
}

func buildCHU13BaseIndex(documents []chU13BenchmarkDocument) *TokenPostingsIndex {
	index := NewTokenPostingsIndex()
	for _, document := range documents {
		index.Upsert(document.row, document.text)
	}
	return index
}

func BenchmarkCHU13BaseMatchAll(b *testing.B) {
	index := buildCHU13BaseIndex(chU13BenchmarkDocuments())
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_ = index.MatchAll("quick brown fox")
	}
}

func BenchmarkCHU13BaseUpsert(b *testing.B) {
	index := NewTokenPostingsIndex()
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
