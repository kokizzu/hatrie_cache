package hatDataStructure

import (
	"unicode"
	"unicode/utf8"

	"hatrie_cache/hat/hatHash"
)

// TokenBloomFilterSnapshot is the compact snapshot format shared with a
// BloomFilter. The tokenizer rules are part of the TokenBloomFilter contract.
type TokenBloomFilterSnapshot = BloomFilterSnapshot

// TokenBloomFilter is a compact, probabilistic prefilter for word-oriented
// text search. It records Unicode letter and digit tokens using simple Unicode
// lower-casing; punctuation and whitespace separate tokens.
//
// It is useful as a segment or document prefilter before an exact text check:
// false positives are possible, but inserted tokens never produce false
// negatives. The zero value is valid and represents an unconfigured filter.
type TokenBloomFilter struct {
	bloom BloomFilter
}

// NewTokenBloomFilter creates a token filter sized for expectedTokens at the
// requested false-positive rate.
func NewTokenBloomFilter(expectedTokens uint64, falsePositiveRate float64) (TokenBloomFilter, error) {
	bloom, err := NewBloomFilter(expectedTokens, falsePositiveRate)
	if err != nil {
		return TokenBloomFilter{}, err
	}
	return TokenBloomFilter{bloom: bloom}, nil
}

// NewTokenBloomFilterWithShape creates a token filter with an explicit bit and
// hash count.
func NewTokenBloomFilterWithShape(bitCount uint64, hashCount uint8) (TokenBloomFilter, error) {
	bloom, err := NewBloomFilterWithShape(bitCount, hashCount)
	if err != nil {
		return TokenBloomFilter{}, err
	}
	return TokenBloomFilter{bloom: bloom}, nil
}

// ValidateTokenBloomFilterSnapshot validates a compact token-filter snapshot.
func ValidateTokenBloomFilterSnapshot(snapshot TokenBloomFilterSnapshot) error {
	return ValidateBloomFilterSnapshot(snapshot)
}

// NewTokenBloomFilterFromSnapshot restores a validated token-filter snapshot.
func NewTokenBloomFilterFromSnapshot(snapshot TokenBloomFilterSnapshot) (TokenBloomFilter, error) {
	bloom, err := NewBloomFilterFromSnapshot(snapshot)
	if err != nil {
		return TokenBloomFilter{}, err
	}
	return TokenBloomFilter{bloom: bloom}, nil
}

// AddText records every Unicode letter-or-digit token in text. It returns true
// when at least one Bloom bit changed.
func (filter *TokenBloomFilter) AddText(text string) bool {
	if filter == nil || filter.bloom.bitCount == 0 || filter.bloom.hashCount == 0 {
		return false
	}
	changed := false
	start := -1
	for index := 0; index < len(text); {
		runeValue, size := utf8.DecodeRuneInString(text[index:])
		if isTokenRune(runeValue) {
			if start < 0 {
				start = index
			}
		} else if start >= 0 {
			if filter.addTokenRange(text[start:index]) {
				changed = true
			}
			start = -1
		}
		index += size
	}
	if start >= 0 && filter.addTokenRange(text[start:]) {
		changed = true
	}
	return changed
}

// AddToken records one Unicode letter-or-digit token after simple Unicode
// lower-casing. Empty or punctuation-containing values are ignored.
func (filter *TokenBloomFilter) AddToken(token string) bool {
	if filter == nil || !isSingleToken(token) {
		return false
	}
	return filter.addTokenRange(token)
}

// ContainsToken reports whether one token may have been recorded. It returns
// false for empty or punctuation-containing values.
func (filter TokenBloomFilter) ContainsToken(token string) bool {
	if !isSingleToken(token) || len(filter.bloom.RawWords()) == 0 {
		return false
	}
	first, step := tokenHashes(token)
	return filter.bloom.containsHashed(first, step)
}

// ContainsAllTokens reports whether every token in text may have been
// recorded. An empty token query returns true because it imposes no filter.
func (filter TokenBloomFilter) ContainsAllTokens(text string) bool {
	start := -1
	for index := 0; index < len(text); {
		runeValue, size := utf8.DecodeRuneInString(text[index:])
		if isTokenRune(runeValue) {
			if start < 0 {
				start = index
			}
		} else if start >= 0 {
			if !filter.containsTokenRange(text[start:index]) {
				return false
			}
			start = -1
		}
		index += size
	}
	return start < 0 || filter.containsTokenRange(text[start:])
}

// ContainsAnyTokens reports whether at least one token in text may have been
// recorded. It returns false when text contains no tokens.
func (filter TokenBloomFilter) ContainsAnyTokens(text string) bool {
	start := -1
	for index := 0; index < len(text); {
		runeValue, size := utf8.DecodeRuneInString(text[index:])
		if isTokenRune(runeValue) {
			if start < 0 {
				start = index
			}
		} else if start >= 0 {
			if filter.containsTokenRange(text[start:index]) {
				return true
			}
			start = -1
		}
		index += size
	}
	return start >= 0 && filter.containsTokenRange(text[start:])
}

// Info reports the underlying Bloom shape and fill level.
func (filter TokenBloomFilter) Info() BloomFilterInfo { return filter.bloom.Info() }

// Snapshot returns the compact snapshot format used by BloomFilter.
func (filter TokenBloomFilter) Snapshot() TokenBloomFilterSnapshot { return filter.bloom.Snapshot() }

// EncodedSize returns the populated bitset size in bytes.
func (filter TokenBloomFilter) EncodedSize() int64 { return filter.bloom.EncodedSize() }

// BitCount returns the configured number of usable bits.
func (filter TokenBloomFilter) BitCount() uint64 { return filter.bloom.BitCount() }

// HashCount returns the configured number of Bloom hash probes.
func (filter TokenBloomFilter) HashCount() uint8 { return filter.bloom.HashCount() }

// Insertions returns the number of token additions that changed at least one
// bit.
func (filter TokenBloomFilter) Insertions() uint64 { return filter.bloom.Insertions() }

// RawWords returns the underlying words without copying. Callers must not
// retain or mutate the returned slice.
func (filter TokenBloomFilter) RawWords() []uint64 { return filter.bloom.RawWords() }

func (filter *TokenBloomFilter) addTokenRange(token string) bool {
	first, step := tokenHashes(token)
	return filter.bloom.addHashed(first, step)
}

func (filter TokenBloomFilter) containsTokenRange(token string) bool {
	if len(filter.bloom.RawWords()) == 0 {
		return false
	}
	first, step := tokenHashes(token)
	return filter.bloom.containsHashed(first, step)
}

func isSingleToken(token string) bool {
	if token == "" {
		return false
	}
	for _, runeValue := range token {
		if !isTokenRune(runeValue) {
			return false
		}
	}
	return true
}

func isTokenRune(runeValue rune) bool {
	return unicode.IsLetter(runeValue) || unicode.IsDigit(runeValue)
}

func tokenHashes(token string) (uint64, uint64) {
	first, step := hatHash.FNVOffset64, hatHash.FNVOffset64
	for _, runeValue := range token {
		runeValue = unicode.ToLower(runeValue)
		switch {
		case runeValue < utf8.RuneSelf:
			first, step = tokenHashByte(first, step, byte(runeValue))
		case runeValue < 1<<11:
			first, step = tokenHashByte(first, step, 0xc0|byte(runeValue>>6))
			first, step = tokenHashByte(first, step, 0x80|byte(runeValue&0x3f))
		case runeValue < 1<<16:
			first, step = tokenHashByte(first, step, 0xe0|byte(runeValue>>12))
			first, step = tokenHashByte(first, step, 0x80|byte((runeValue>>6)&0x3f))
			first, step = tokenHashByte(first, step, 0x80|byte(runeValue&0x3f))
		default:
			first, step = tokenHashByte(first, step, 0xf0|byte(runeValue>>18))
			first, step = tokenHashByte(first, step, 0x80|byte((runeValue>>12)&0x3f))
			first, step = tokenHashByte(first, step, 0x80|byte((runeValue>>6)&0x3f))
			first, step = tokenHashByte(first, step, 0x80|byte(runeValue&0x3f))
		}
	}
	return first, step
}

func tokenHashByte(first, step uint64, value byte) (uint64, uint64) {
	first ^= uint64(value)
	first *= hatHash.FNVPrime64
	step *= hatHash.FNVPrime64
	step ^= uint64(value)
	return first, step
}
