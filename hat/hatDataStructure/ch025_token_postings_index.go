package hatDataStructure

import (
	"math/bits"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// TokenPostingsIndexInfo describes the active portion of a token postings
// index. Postings is the total number of row-token memberships.
type TokenPostingsIndexInfo struct {
	Rows         uint64
	Terms        uint64
	Postings     uint64
	EncodedBytes uint64
}

// TokenPostingsIndex is an exact, concurrent token-to-row index for text
// predicates. Tokens are lower-cased Unicode letters and numbers; punctuation
// and whitespace separate tokens. The index is derived state: callers should
// rebuild it from source rows after persistence restore.
//
// Row IDs are uint32 to match RoaringBitmap. Upsert replaces all memberships
// previously stored for the row. A query holds a read lock while its callback
// runs, so callbacks must not call back into the same index.
type TokenPostingsIndex struct {
	mu sync.RWMutex

	termIDs     map[string]uint32
	terms       []string
	postings    []RoaringBitmap
	termRefRows []uint64
	freeTermIDs []uint32
	rows        map[uint32][]uint32
	phrase      *tokenPhrasePostingsState
}

// NewTokenPostingsIndex creates an empty token postings index.
func NewTokenPostingsIndex() *TokenPostingsIndex {
	return &TokenPostingsIndex{
		termIDs: make(map[string]uint32),
		rows:    make(map[uint32][]uint32),
	}
}

// Upsert replaces the normalized token set associated with row.
func (index *TokenPostingsIndex) Upsert(row uint32, text string) {
	if index == nil {
		return
	}
	var orderedTokens []string
	var tokens []string
	if index.phrase != nil {
		orderedTokens = tokenPostingsOrderedTokens(text)
		tokens = tokenPostingsUniqueTokens(append([]string(nil), orderedTokens...))
	} else {
		tokens = tokenPostingsTokens(text)
	}
	if len(tokens) == 0 {
		index.Delete(row)
		return
	}

	index.mu.Lock()
	defer index.mu.Unlock()
	index.ensureInitializedLocked()

	termIDs := index.resolveTermIDsLocked(tokens)
	var orderedTermIDs []uint32
	var encodedPhrase []byte
	if index.phrase != nil {
		orderedTermIDs = index.lookupOrderedTermIDsLocked(orderedTokens)
		encodedPhrase = encodeTokenPhrase(orderedTermIDs)
	}
	oldTermIDs, exists := index.rows[row]
	if exists && sameUint32Slice(oldTermIDs, termIDs) &&
		(index.phrase == nil || sameTokenPhraseSequence(index.phrase.sequences[row], encodedPhrase)) {
		return
	}
	if exists {
		if index.phrase != nil {
			index.removePhraseSequenceLocked(row, index.phrase.sequences[row])
		}
		index.removeRowTermsLocked(row, oldTermIDs)
	}
	for _, termID := range termIDs {
		index.postings[termID].Add(row)
		index.termRefRows[termID]++
	}
	index.rows[row] = termIDs
	if index.phrase != nil {
		index.phrase.sequences[row] = encodedPhrase
		index.addPhraseSequenceLocked(row, orderedTermIDs)
	}
}

// Delete removes row and returns whether it was indexed.
func (index *TokenPostingsIndex) Delete(row uint32) bool {
	if index == nil {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if index.rows == nil {
		return false
	}
	termIDs, exists := index.rows[row]
	if !exists {
		return false
	}
	if index.phrase != nil {
		index.removePhraseSequenceLocked(row, index.phrase.sequences[row])
	}
	index.removeRowTermsLocked(row, termIDs)
	delete(index.rows, row)
	return true
}

// RowsForToken returns matching row IDs in ascending order. It returns nil
// when token is missing or contains more than one normalized token.
func (index *TokenPostingsIndex) RowsForToken(token string) []uint32 {
	return index.RowsForTokenInto(nil, token)
}

// RowsForTokenInto appends matching row IDs to dst without clearing dst.
func (index *TokenPostingsIndex) RowsForTokenInto(dst []uint32, token string) []uint32 {
	if index == nil {
		return dst
	}
	normalizedToken, ok := tokenPostingsSingleToken(token)
	if !ok {
		return dst
	}

	index.mu.RLock()
	defer index.mu.RUnlock()
	termID, exists := index.termIDs[normalizedToken]
	if !exists {
		return dst
	}
	return appendTokenPostingRows(dst, index.postings[termID])
}

// MatchAll returns rows containing every normalized token in query, in
// ascending order. An empty or tokenless query contributes no rows.
func (index *TokenPostingsIndex) MatchAll(query string) []uint32 {
	return index.MatchAllInto(nil, query)
}

// MatchAllInto appends rows containing every normalized token in query to dst.
func (index *TokenPostingsIndex) MatchAllInto(dst []uint32, query string) []uint32 {
	if index == nil {
		return dst
	}
	tokens := tokenPostingsTokens(query)
	if len(tokens) == 0 {
		return dst
	}

	index.mu.RLock()
	defer index.mu.RUnlock()
	termIDs := index.lookupTermIDsLocked(tokens)
	if len(termIDs) != len(tokens) {
		return dst
	}
	candidateIndex := 0
	for termIndex := 1; termIndex < len(termIDs); termIndex++ {
		if index.postings[termIDs[termIndex]].Count() < index.postings[termIDs[candidateIndex]].Count() {
			candidateIndex = termIndex
		}
	}
	candidate := index.postings[termIDs[candidateIndex]]
	others := make([]RoaringBitmap, 0, len(termIDs)-1)
	for termIndex, termID := range termIDs {
		if termIndex != candidateIndex {
			others = append(others, index.postings[termID])
		}
	}
	dst = growTokenPostingRows(dst, candidate.Count())
	visitTokenPostingRows(candidate, func(row uint32) bool {
		for _, posting := range others {
			if !posting.Contains(row) {
				return true
			}
		}
		dst = append(dst, row)
		return true
	})
	return dst
}

// MatchAny returns rows containing at least one normalized token in query, in
// ascending order. An empty or tokenless query contributes no rows.
func (index *TokenPostingsIndex) MatchAny(query string) []uint32 {
	return index.MatchAnyInto(nil, query)
}

// MatchAnyInto appends rows containing at least one normalized token in query
// to dst.
func (index *TokenPostingsIndex) MatchAnyInto(dst []uint32, query string) []uint32 {
	if index == nil {
		return dst
	}
	tokens := tokenPostingsTokens(query)
	if len(tokens) == 0 {
		return dst
	}

	index.mu.RLock()
	defer index.mu.RUnlock()
	termIDs := index.lookupTermIDsLocked(tokens)
	if len(termIDs) == 0 {
		return dst
	}
	if len(termIDs) == 1 {
		return appendTokenPostingRows(dst, index.postings[termIDs[0]])
	}
	union := NewRoaringBitmap()
	for _, termID := range termIDs {
		visitTokenPostingRows(index.postings[termID], func(row uint32) bool {
			union.Add(row)
			return true
		})
	}
	return appendTokenPostingRows(dst, union)
}

// VisitToken visits matching row IDs in ascending order without materializing
// a result slice. It returns false when the callback stops the visit.
func (index *TokenPostingsIndex) VisitToken(token string, visit func(row uint32) bool) bool {
	if index == nil || visit == nil {
		return true
	}
	normalizedToken, ok := tokenPostingsSingleToken(token)
	if !ok {
		return true
	}

	index.mu.RLock()
	defer index.mu.RUnlock()
	termID, exists := index.termIDs[normalizedToken]
	if !exists {
		return true
	}
	return visitTokenPostingRows(index.postings[termID], visit)
}

// Len returns the number of rows with at least one indexed token.
func (index *TokenPostingsIndex) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.rows)
}

// TermCount returns the number of active normalized terms.
func (index *TokenPostingsIndex) TermCount() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.termIDs)
}

// Info reports active rows, terms, memberships, and Roaring payload bytes.
func (index *TokenPostingsIndex) Info() TokenPostingsIndexInfo {
	if index == nil {
		return TokenPostingsIndexInfo{}
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	info := TokenPostingsIndexInfo{Rows: uint64(len(index.rows)), Terms: uint64(len(index.termIDs))}
	for termID, term := range index.terms {
		if term == "" {
			continue
		}
		info.Postings += index.termRefRows[termID]
		info.EncodedBytes += uint64(index.postings[termID].EncodedSize())
	}
	return info
}

// Clear removes all rows and terms while retaining the index value for reuse.
func (index *TokenPostingsIndex) Clear() {
	if index == nil {
		return
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	index.termIDs = nil
	index.terms = nil
	index.postings = nil
	index.termRefRows = nil
	index.freeTermIDs = nil
	index.rows = nil
	if index.phrase != nil {
		index.phrase.sequences = make(map[uint32][]byte)
		index.phrase.postings = make(map[uint64]RoaringBitmap)
	}
}

func (index *TokenPostingsIndex) ensureInitializedLocked() {
	if index.termIDs == nil {
		index.termIDs = make(map[string]uint32)
	}
	if index.rows == nil {
		index.rows = make(map[uint32][]uint32)
	}
	if index.phrase != nil {
		index.phrase.ensureInitialized()
	}
}

func (index *TokenPostingsIndex) resolveTermIDsLocked(tokens []string) []uint32 {
	termIDs := make([]uint32, len(tokens))
	for tokenIndex, token := range tokens {
		if termID, exists := index.termIDs[token]; exists {
			termIDs[tokenIndex] = termID
			continue
		}
		var termID uint32
		if freeCount := len(index.freeTermIDs); freeCount > 0 {
			termID = index.freeTermIDs[freeCount-1]
			index.freeTermIDs = index.freeTermIDs[:freeCount-1]
			index.terms[termID] = strings.Clone(token)
			index.postings[termID] = NewRoaringBitmap()
			index.termRefRows[termID] = 0
		} else {
			termID = uint32(len(index.terms))
			index.terms = append(index.terms, strings.Clone(token))
			index.postings = append(index.postings, NewRoaringBitmap())
			index.termRefRows = append(index.termRefRows, 0)
		}
		index.termIDs[index.terms[termID]] = termID
		termIDs[tokenIndex] = termID
	}
	return termIDs
}

func (index *TokenPostingsIndex) lookupTermIDsLocked(tokens []string) []uint32 {
	termIDs := make([]uint32, 0, len(tokens))
	for _, token := range tokens {
		termID, exists := index.termIDs[token]
		if !exists {
			continue
		}
		termIDs = append(termIDs, termID)
	}
	return termIDs
}

func (index *TokenPostingsIndex) lookupOrderedTermIDsLocked(tokens []string) []uint32 {
	termIDs := make([]uint32, len(tokens))
	for tokenIndex, token := range tokens {
		termIDs[tokenIndex] = index.termIDs[token]
	}
	return termIDs
}

func (index *TokenPostingsIndex) removeRowTermsLocked(row uint32, termIDs []uint32) {
	for _, termID := range termIDs {
		index.postings[termID].Remove(row)
		index.termRefRows[termID]--
		if index.termRefRows[termID] != 0 {
			continue
		}
		term := index.terms[termID]
		delete(index.termIDs, term)
		index.terms[termID] = ""
		index.postings[termID] = NewRoaringBitmap()
		index.freeTermIDs = append(index.freeTermIDs, termID)
	}
}

func tokenPostingsTokens(text string) []string {
	return tokenPostingsUniqueTokens(tokenPostingsOrderedTokens(text))
}

func tokenPostingsOrderedTokens(text string) []string {
	tokens := make([]string, 0, 8)
	start := -1
	for offset, runeValue := range text {
		if isTokenPostingsRune(runeValue) {
			if start < 0 {
				start = offset
			}
			continue
		}
		if start >= 0 {
			tokens = append(tokens, strings.ToLower(text[start:offset]))
			start = -1
		}
	}
	if start >= 0 {
		tokens = append(tokens, strings.ToLower(text[start:]))
	}
	return tokens
}

func tokenPostingsUniqueTokens(tokens []string) []string {
	if len(tokens) < 2 {
		return tokens
	}
	sort.Strings(tokens)
	unique := 1
	for tokenIndex := 1; tokenIndex < len(tokens); tokenIndex++ {
		if tokens[tokenIndex] == tokens[unique-1] {
			continue
		}
		tokens[unique] = tokens[tokenIndex]
		unique++
	}
	return tokens[:unique]
}

func tokenPostingsSingleToken(text string) (string, bool) {
	start := -1
	normalized := ""
	for offset, runeValue := range text {
		if isTokenPostingsRune(runeValue) {
			if start < 0 {
				start = offset
			}
			continue
		}
		if start < 0 {
			continue
		}
		if normalized != "" {
			return "", false
		}
		normalized = strings.ToLower(text[start:offset])
		start = -1
	}
	if start >= 0 {
		if normalized != "" {
			return "", false
		}
		normalized = strings.ToLower(text[start:])
	}
	return normalized, normalized != ""
}

func isTokenPostingsRune(runeValue rune) bool {
	return unicode.IsLetter(runeValue) || unicode.IsDigit(runeValue)
}

func sameUint32Slice(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func appendTokenPostingRows(dst []uint32, posting RoaringBitmap) []uint32 {
	dst = growTokenPostingRows(dst, posting.Count())
	visitTokenPostingRows(posting, func(row uint32) bool {
		dst = append(dst, row)
		return true
	})
	return dst
}

func growTokenPostingRows(dst []uint32, additional uint64) []uint32 {
	maxInt := uint64(^uint(0) >> 1)
	if additional > maxInt-uint64(len(dst)) {
		return dst
	}
	required := len(dst) + int(additional)
	if required <= cap(dst) {
		return dst
	}
	grown := make([]uint32, len(dst), required)
	copy(grown, dst)
	return grown
}

func visitTokenPostingRows(posting RoaringBitmap, visit func(row uint32) bool) bool {
	completed := true
	posting.VisitContainers(func(key uint16, _ uint32, values []uint16, bitset []uint64) bool {
		base := uint32(key) << 16
		if values != nil {
			for _, value := range values {
				if !visit(base | uint32(value)) {
					completed = false
					return false
				}
			}
			return true
		}
		for wordIndex, word := range bitset {
			for word != 0 {
				bitIndex := uint(bits.TrailingZeros64(word))
				if !visit(base | uint32(wordIndex*64) | uint32(bitIndex)) {
					completed = false
					return false
				}
				word &= word - 1
			}
		}
		return true
	})
	return completed
}
