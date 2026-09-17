package hatDataStructure

import (
	"container/heap"
	"errors"
	"math"
	"sort"
)

const (
	// DefaultTokenPostingsRankLimit bounds a ranked query when no limit is set.
	DefaultTokenPostingsRankLimit = 100
	// MaxTokenPostingsRankLimit prevents accidental unbounded result retention.
	MaxTokenPostingsRankLimit = 10_000
	// DefaultTokenPostingsRankK1 is the standard BM25 term-frequency saturation.
	DefaultTokenPostingsRankK1 = 1.2
	// DefaultTokenPostingsRankB is the standard BM25 document-length normalization.
	DefaultTokenPostingsRankB = 0.75
)

var (
	// ErrTokenPostingsRankingDisabled reports use of ranked search on a plain index.
	ErrTokenPostingsRankingDisabled = errors.New("token postings ranking is disabled")
	// ErrTokenPostingsInvalidRankOptions reports an invalid ranking option.
	ErrTokenPostingsInvalidRankOptions = errors.New("invalid token postings rank options")
)

// TokenPostingsIndexOptions controls optional derived state on a token
// postings index. The default is deliberately memory-minimal.
type TokenPostingsIndexOptions struct {
	// TrackTermFrequency enables document lengths and per-term frequencies for
	// MatchRanked. It adds memory and update work only when explicitly enabled.
	TrackTermFrequency bool
}

// NewTokenPostingsIndexWithOptions creates an index with explicit optional
// derived-state settings.
func NewTokenPostingsIndexWithOptions(options TokenPostingsIndexOptions) *TokenPostingsIndex {
	index := &TokenPostingsIndex{
		termIDs: make(map[string]uint32),
		rows:    make(map[uint32][]uint32),
	}
	if options.TrackTermFrequency {
		index.ranking = &tokenPostingsRankingState{
			documents: make(map[uint32]tokenPostingsRankDocument),
		}
	}
	return index
}

// NewRankedTokenPostingsIndex creates a token postings index with the optional
// frequency metadata required by MatchRanked.
func NewRankedTokenPostingsIndex() *TokenPostingsIndex {
	return NewTokenPostingsIndexWithOptions(TokenPostingsIndexOptions{TrackTermFrequency: true})
}

// TokenPostingsRankOptions controls BM25-like ranked token matching. A zero
// value uses the conservative default limit and standard k1/b values. A zero
// B therefore selects the default; use a small positive value when a custom
// near-zero normalization factor is required.
type TokenPostingsRankOptions struct {
	Limit int
	K1    float64
	B     float64
}

// TokenPostingsRankedRow is one row and its deterministic BM25-like score.
type TokenPostingsRankedRow struct {
	Row   uint32
	Score float64
}

type tokenPostingsRankingState struct {
	documents   map[uint32]tokenPostingsRankDocument
	totalLength uint64
}

type tokenPostingsRankDocument struct {
	length      uint32
	frequencies []uint32
}

type tokenPostingsRankTerm struct {
	termID      uint32
	queryWeight float64
	idf         float64
}

// MatchRanked returns up to options.Limit rows containing at least one query
// token, ordered by descending BM25-like score and ascending row ID for ties.
// Ranking is opt-in through NewRankedTokenPostingsIndex because retaining
// frequencies is unnecessary for exact filtering.
func (index *TokenPostingsIndex) MatchRanked(query string, options TokenPostingsRankOptions) ([]TokenPostingsRankedRow, error) {
	return index.MatchRankedInto(nil, query, options)
}

// MatchRankedInto appends ranked matches to dst, preserving its existing
// prefix. The limit applies only to rows appended by this call.
func (index *TokenPostingsIndex) MatchRankedInto(dst []TokenPostingsRankedRow, query string, options TokenPostingsRankOptions) ([]TokenPostingsRankedRow, error) {
	if index == nil {
		return dst, nil
	}
	tokens := tokenPostingsOrderedTokens(query)
	if len(tokens) == 0 {
		return dst, nil
	}
	queryTokens, queryFrequencies := tokenPostingsUniqueTokenFrequencies(tokens)
	limit, k1, b, err := options.normalized()
	if err != nil {
		return dst, err
	}

	index.mu.RLock()
	defer index.mu.RUnlock()
	if index.ranking == nil {
		return dst, ErrTokenPostingsRankingDisabled
	}
	if len(index.ranking.documents) == 0 {
		return dst, nil
	}

	terms := make([]tokenPostingsRankTerm, 0, len(queryTokens))
	queryPositions := make(map[uint32]int, len(queryTokens))
	documentCount := float64(len(index.ranking.documents))
	averageLength := float64(index.ranking.totalLength) / documentCount
	if averageLength <= 0 {
		return dst, nil
	}
	for tokenIndex, token := range queryTokens {
		termID, exists := index.termIDs[token]
		if !exists {
			continue
		}
		documentFrequency := float64(index.termRefRows[termID])
		idf := math.Log1p((documentCount - documentFrequency + 0.5) / (documentFrequency + 0.5))
		term := tokenPostingsRankTerm{
			termID:      termID,
			queryWeight: tokenPostingsBM25QueryWeight(queryFrequencies[tokenIndex]),
			idf:         idf,
		}
		queryPositions[termID] = len(terms)
		terms = append(terms, term)
	}
	if len(terms) == 0 {
		return dst, nil
	}

	candidates := NewRoaringBitmap()
	for _, term := range terms {
		visitTokenPostingRows(index.postings[term.termID], func(row uint32) bool {
			candidates.Add(row)
			return true
		})
	}

	top := rankedTokenPostingsHeap{}
	heap.Init(&top)
	visitTokenPostingRows(candidates, func(row uint32) bool {
		document, exists := index.ranking.documents[row]
		if !exists {
			return true
		}
		score := tokenPostingsBM25Score(index.rows[row], document, terms, queryPositions, averageLength, k1, b)
		if score <= 0 {
			return true
		}
		candidate := TokenPostingsRankedRow{Row: row, Score: score}
		if top.Len() < limit {
			heap.Push(&top, candidate)
			return true
		}
		if rankedTokenPostingsBetter(candidate, top[0]) {
			top[0] = candidate
			heap.Fix(&top, 0)
		}
		return true
	})

	results := make([]TokenPostingsRankedRow, top.Len())
	copy(results, top)
	sort.Slice(results, func(left, right int) bool {
		return rankedTokenPostingsBetter(results[left], results[right])
	})
	return append(dst, results...), nil
}

func (options TokenPostingsRankOptions) normalized() (int, float64, float64, error) {
	limit := options.Limit
	if limit < 0 || limit > MaxTokenPostingsRankLimit {
		return 0, 0, 0, ErrTokenPostingsInvalidRankOptions
	}
	if limit == 0 {
		limit = DefaultTokenPostingsRankLimit
	}
	k1 := options.K1
	if k1 == 0 {
		k1 = DefaultTokenPostingsRankK1
	}
	if math.IsNaN(k1) || math.IsInf(k1, 0) || k1 <= 0 {
		return 0, 0, 0, ErrTokenPostingsInvalidRankOptions
	}
	b := options.B
	if b == 0 {
		b = DefaultTokenPostingsRankB
	}
	if math.IsNaN(b) || math.IsInf(b, 0) || b < 0 || b > 1 {
		return 0, 0, 0, ErrTokenPostingsInvalidRankOptions
	}
	return limit, k1, b, nil
}

func tokenPostingsUniqueTokenFrequencies(tokens []string) ([]string, []uint32) {
	if len(tokens) == 0 {
		return nil, nil
	}
	sort.Strings(tokens)
	frequencies := make([]uint32, len(tokens))
	unique := 0
	for _, token := range tokens {
		if unique > 0 && tokens[unique-1] == token {
			frequencies[unique-1]++
			continue
		}
		tokens[unique] = token
		frequencies[unique] = 1
		unique++
	}
	return tokens[:unique], frequencies[:unique]
}

func tokenPostingsDocumentLength(length int) uint32 {
	maxUint32 := uint64(^uint32(0))
	if uint64(length) > maxUint32 {
		return ^uint32(0)
	}
	return uint32(length)
}

func tokenPostingsBM25QueryWeight(frequency uint32) float64 {
	if frequency == 0 {
		return 1
	}
	// A small k3 gives repeated query tokens a bounded influence without
	// allowing a duplicated query string to dominate document relevance.
	const k3 = 8.0
	return (k3 + 1) * float64(frequency) / (k3 + float64(frequency))
}

func tokenPostingsBM25Score(rowTerms []uint32, document tokenPostingsRankDocument, terms []tokenPostingsRankTerm, queryPositions map[uint32]int, averageLength, k1, b float64) float64 {
	normalization := k1 * (1 - b + b*float64(document.length)/averageLength)
	if normalization <= 0 {
		normalization = k1
	}
	score := 0.0
	for termIndex, termID := range rowTerms {
		queryIndex, exists := queryPositions[termID]
		if !exists || termIndex >= len(document.frequencies) {
			continue
		}
		frequency := float64(document.frequencies[termIndex])
		if frequency <= 0 {
			continue
		}
		term := terms[queryIndex]
		score += term.idf * term.queryWeight * frequency * (k1 + 1) / (frequency + normalization)
	}
	return score
}

func sameTokenPostingsRankDocument(document tokenPostingsRankDocument, frequencies []uint32, length uint32) bool {
	if document.length != length || len(document.frequencies) != len(frequencies) {
		return false
	}
	for index, frequency := range frequencies {
		if document.frequencies[index] != frequency {
			return false
		}
	}
	return true
}

func (index *TokenPostingsIndex) removeRankDocumentLocked(row uint32) {
	document, exists := index.ranking.documents[row]
	if !exists {
		return
	}
	if index.ranking.totalLength >= uint64(document.length) {
		index.ranking.totalLength -= uint64(document.length)
	} else {
		index.ranking.totalLength = 0
	}
	delete(index.ranking.documents, row)
}

func rankedTokenPostingsBetter(left, right TokenPostingsRankedRow) bool {
	if left.Score != right.Score {
		return left.Score > right.Score
	}
	return left.Row < right.Row
}

type rankedTokenPostingsHeap []TokenPostingsRankedRow

func (heap rankedTokenPostingsHeap) Len() int { return len(heap) }

func (heap rankedTokenPostingsHeap) Less(left, right int) bool {
	return rankedTokenPostingsBetter(heap[right], heap[left])
}

func (heap rankedTokenPostingsHeap) Swap(left, right int) {
	heap[left], heap[right] = heap[right], heap[left]
}

func (heap *rankedTokenPostingsHeap) Push(value any) {
	*heap = append(*heap, value.(TokenPostingsRankedRow))
}

func (heap *rankedTokenPostingsHeap) Pop() any {
	old := *heap
	last := len(old) - 1
	value := old[last]
	*heap = old[:last]
	return value
}
