package hatDataStructure

import (
	"bytes"
	"encoding/binary"
)

// tokenPhrasePostingsState is allocated only by the phrase-enabled
// constructor. The base token index remains order-independent and compact.
type tokenPhrasePostingsState struct {
	sequences map[uint32][]byte
	postings  map[uint64]RoaringBitmap
}

func (state *tokenPhrasePostingsState) ensureInitialized() {
	if state.sequences == nil {
		state.sequences = make(map[uint32][]byte)
	}
	if state.postings == nil {
		state.postings = make(map[uint64]RoaringBitmap)
	}
}

// NewTokenPostingsIndexWithPhrases creates an exact token index with
// contiguous phrase matching enabled. Phrase state is opt-in because it
// retains token order and therefore uses more memory than the base index.
func NewTokenPostingsIndexWithPhrases() *TokenPostingsIndex {
	index := NewTokenPostingsIndex()
	index.phrase = &tokenPhrasePostingsState{
		sequences: make(map[uint32][]byte),
		postings:  make(map[uint64]RoaringBitmap),
	}
	return index
}

// TokenPhrasePostingsIndexInfo describes the payload retained by the optional
// phrase state. Map and allocator overhead are not included in the byte
// counters; they are intended for relative measurements between configurations.
type TokenPhrasePostingsIndexInfo struct {
	Rows          uint64
	Tokens        uint64
	Bigrams       uint64
	SequenceBytes uint64
	EncodedBytes  uint64
}

// PhraseInfo reports the optional phrase sequence and bigram posting payload.
func (index *TokenPostingsIndex) PhraseInfo() TokenPhrasePostingsIndexInfo {
	if index == nil || index.phrase == nil {
		return TokenPhrasePostingsIndexInfo{}
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	info := TokenPhrasePostingsIndexInfo{
		Rows:    uint64(len(index.phrase.sequences)),
		Bigrams: uint64(len(index.phrase.postings)),
	}
	for _, sequence := range index.phrase.sequences {
		info.SequenceBytes += uint64(len(sequence))
		for offset := 0; offset < len(sequence); {
			_, size := binary.Uvarint(sequence[offset:])
			if size <= 0 {
				break
			}
			info.Tokens++
			offset += size
		}
	}
	for _, posting := range index.phrase.postings {
		info.EncodedBytes += uint64(posting.EncodedSize())
	}
	return info
}

// MatchPhrase returns rows containing the normalized query as contiguous
// tokens, in ascending row order. An empty or tokenless query contributes no
// rows. A phrase-enabled index is required; a base index returns nil.
func (index *TokenPostingsIndex) MatchPhrase(query string) []uint32 {
	return index.MatchPhraseInto(nil, query)
}

// MatchPhraseInto appends exact contiguous phrase matches to dst.
func (index *TokenPostingsIndex) MatchPhraseInto(dst []uint32, query string) []uint32 {
	if index == nil || index.phrase == nil {
		return dst
	}
	tokens := tokenPostingsOrderedTokens(query)
	if len(tokens) == 0 {
		return dst
	}

	index.mu.RLock()
	defer index.mu.RUnlock()
	termIDs, ok := index.lookupPhraseTermIDsLocked(tokens)
	if !ok {
		return dst
	}
	return index.appendPhraseMatchesLocked(dst, termIDs)
}

// VisitPhrase visits exact contiguous phrase matches in ascending row order
// without materializing a result slice. It returns false when the callback
// stops the visit.
func (index *TokenPostingsIndex) VisitPhrase(query string, visit func(row uint32) bool) bool {
	if index == nil || visit == nil || index.phrase == nil {
		return true
	}
	tokens := tokenPostingsOrderedTokens(query)
	if len(tokens) == 0 {
		return true
	}

	index.mu.RLock()
	defer index.mu.RUnlock()
	termIDs, ok := index.lookupPhraseTermIDsLocked(tokens)
	if !ok {
		return true
	}
	return index.visitPhraseMatchesLocked(termIDs, visit)
}

func (index *TokenPostingsIndex) lookupPhraseTermIDsLocked(tokens []string) ([]uint32, bool) {
	termIDs := make([]uint32, len(tokens))
	for tokenIndex, token := range tokens {
		termID, exists := index.termIDs[token]
		if !exists {
			return nil, false
		}
		termIDs[tokenIndex] = termID
	}
	return termIDs, true
}

func (index *TokenPostingsIndex) appendPhraseMatchesLocked(dst []uint32, termIDs []uint32) []uint32 {
	if len(termIDs) == 1 {
		return appendTokenPostingRows(dst, index.postings[termIDs[0]])
	}
	if len(termIDs) < 2 {
		return dst
	}
	candidate, ok := index.rarestPhrasePostingLocked(termIDs)
	if !ok {
		return dst
	}
	dst = growTokenPostingRows(dst, candidate.Count())
	phraseBytes := encodeTokenPhrase(termIDs)
	index.visitPhraseCandidatesLocked(candidate, termIDs, phraseBytes, func(row uint32) bool {
		dst = append(dst, row)
		return true
	})
	return dst
}

func (index *TokenPostingsIndex) visitPhraseMatchesLocked(termIDs []uint32, visit func(row uint32) bool) bool {
	if len(termIDs) == 1 {
		return visitTokenPostingRows(index.postings[termIDs[0]], visit)
	}
	if len(termIDs) < 2 {
		return true
	}
	candidate, ok := index.rarestPhrasePostingLocked(termIDs)
	if !ok {
		return true
	}
	return index.visitPhraseCandidatesLocked(candidate, termIDs, encodeTokenPhrase(termIDs), visit)
}

func (index *TokenPostingsIndex) rarestPhrasePostingLocked(termIDs []uint32) (RoaringBitmap, bool) {
	state := index.phrase
	var candidate RoaringBitmap
	bestCount := ^uint64(0)
	for termIndex := 1; termIndex < len(termIDs); termIndex++ {
		key := tokenPhrasePairKey(termIDs[termIndex-1], termIDs[termIndex])
		posting, exists := state.postings[key]
		if !exists {
			return RoaringBitmap{}, false
		}
		count := posting.Count()
		if count < bestCount {
			candidate = posting
			bestCount = count
		}
	}
	return candidate, true
}

func (index *TokenPostingsIndex) visitPhraseCandidatesLocked(candidate RoaringBitmap, termIDs []uint32, phraseBytes []byte, visit func(row uint32) bool) bool {
	state := index.phrase
	return visitTokenPostingRows(candidate, func(row uint32) bool {
		sequence, exists := state.sequences[row]
		if !exists || !containsEncodedTokenPhrase(sequence, phraseBytes) {
			return true
		}
		return visit(row)
	})
}

func containsEncodedTokenPhrase(sequence, phrase []byte) bool {
	if len(phrase) == 0 {
		return true
	}
	for searchStart := 0; searchStart+len(phrase) <= len(sequence); {
		offset := bytes.Index(sequence[searchStart:], phrase)
		if offset < 0 {
			return false
		}
		offset += searchStart
		if offset == 0 || sequence[offset-1]&0x80 == 0 {
			return true
		}
		searchStart = offset + 1
	}
	return false
}

func (index *TokenPostingsIndex) addPhraseSequenceLocked(row uint32, sequence []uint32) {
	state := index.phrase
	for termIndex := 1; termIndex < len(sequence); termIndex++ {
		key := tokenPhrasePairKey(sequence[termIndex-1], sequence[termIndex])
		posting := state.postings[key]
		posting.Add(row)
		state.postings[key] = posting
	}
}

func (index *TokenPostingsIndex) removePhraseSequenceLocked(row uint32, sequence []byte) {
	state := index.phrase
	if state == nil {
		return
	}
	var previous uint32
	hasPrevious := false
	for offset := 0; offset < len(sequence); {
		termID, size := binary.Uvarint(sequence[offset:])
		if size <= 0 {
			break
		}
		current := uint32(termID)
		if hasPrevious {
			index.removePhrasePairLocked(row, tokenPhrasePairKey(previous, current))
		}
		previous = current
		hasPrevious = true
		offset += size
	}
	delete(state.sequences, row)
}

func (index *TokenPostingsIndex) removePhrasePairLocked(row uint32, key uint64) {
	state := index.phrase
	if state == nil {
		return
	}
	posting, exists := state.postings[key]
	if !exists {
		return
	}
	posting.Remove(row)
	if posting.Count() == 0 {
		delete(state.postings, key)
		return
	}
	state.postings[key] = posting
}

func tokenPhrasePairKey(left, right uint32) uint64 {
	return uint64(left)<<32 | uint64(right)
}

func encodeTokenPhrase(sequence []uint32) []byte {
	encoded := make([]byte, 0, len(sequence))
	var buffer [binary.MaxVarintLen32]byte
	for _, termID := range sequence {
		size := binary.PutUvarint(buffer[:], uint64(termID))
		encoded = append(encoded, buffer[:size]...)
	}
	return encoded
}

func sameTokenPhraseSequence(left, right []byte) bool {
	return bytes.Equal(left, right)
}
