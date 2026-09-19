// Package hatMembership provides immutable, exact membership structures for
// constant predicate sets.
package hatMembership

import (
	"errors"
	"sort"
)

const (
	defaultSortedMax   = 32
	defaultBitmapSpan  = 1 << 20
	defaultBitmapRatio = 64
	defaultHashMinSize = 256
)

var ErrInvalidConfig = errors.New("invalid membership-set configuration")

// Mode identifies the representation selected for a set.
type Mode uint8

const (
	ModeEmpty Mode = iota
	ModeSorted
	ModeBitmap
	ModeHash
)

func (m Mode) String() string {
	switch m {
	case ModeEmpty:
		return "empty"
	case ModeSorted:
		return "sorted"
	case ModeBitmap:
		return "bitmap"
	case ModeHash:
		return "hash"
	default:
		return "unknown"
	}
}

// Config controls representation thresholds. Zero values select the defaults.
type Config struct {
	SortedMax   int
	BitmapSpan  uint64
	BitmapRatio uint64
	HashMinSize int
}

// DefaultConfig returns thresholds tuned for exact constant-set lookups.
func DefaultConfig() Config {
	return Config{
		SortedMax:   defaultSortedMax,
		BitmapSpan:  defaultBitmapSpan,
		BitmapRatio: defaultBitmapRatio,
		HashMinSize: defaultHashMinSize,
	}
}

// Uint64Set is immutable after construction and safe for concurrent Contains
// calls. BuildUint64 never mutates its input.
type Uint64Set struct {
	mode   Mode
	length int
	sorted []uint64
	base   uint64
	bitmap []uint64
	hash   map[uint64]struct{}
}

// BuildUint64 selects a compact exact representation using DefaultConfig.
func BuildUint64(values []uint64) *Uint64Set {
	set, err := BuildUint64WithConfig(values, DefaultConfig())
	if err != nil {
		panic(err)
	}
	return set
}

// BuildUint64WithConfig selects sorted-vector, dense-bitmap, or hash mode.
func BuildUint64WithConfig(values []uint64, config Config) (*Uint64Set, error) {
	config = normalizeConfig(config)
	if config.SortedMax < 1 || config.BitmapSpan < 1 || config.BitmapRatio < 1 || config.HashMinSize < 1 {
		return nil, ErrInvalidConfig
	}
	if len(values) == 0 {
		return &Uint64Set{mode: ModeEmpty}, nil
	}
	unique := append([]uint64(nil), values...)
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })
	write := 1
	for _, value := range unique[1:] {
		if value != unique[write-1] {
			unique[write] = value
			write++
		}
	}
	unique = unique[:write]
	set := &Uint64Set{length: len(unique)}
	if len(unique) <= config.SortedMax {
		set.mode = ModeSorted
		set.sorted = unique
		return set, nil
	}

	min, max := unique[0], unique[len(unique)-1]
	if max >= min {
		span := max - min
		if span != ^uint64(0) {
			span++
			if span <= config.BitmapSpan && span <= uint64(len(unique))*config.BitmapRatio {
				words := (span + 63) / 64
				set.mode = ModeBitmap
				set.base = min
				set.bitmap = make([]uint64, words)
				for _, value := range unique {
					offset := value - min
					set.bitmap[offset/64] |= uint64(1) << (offset % 64)
				}
				return set, nil
			}
		}
	}
	if len(unique) >= config.HashMinSize {
		set.mode = ModeHash
		set.hash = make(map[uint64]struct{}, len(unique))
		for _, value := range unique {
			set.hash[value] = struct{}{}
		}
		return set, nil
	}
	set.mode = ModeSorted
	set.sorted = unique
	return set, nil
}

// Contains reports exact membership.
func (s *Uint64Set) Contains(value uint64) bool {
	if s == nil {
		return false
	}
	switch s.mode {
	case ModeSorted:
		index := sort.Search(len(s.sorted), func(i int) bool { return s.sorted[i] >= value })
		return index < len(s.sorted) && s.sorted[index] == value
	case ModeBitmap:
		if value < s.base {
			return false
		}
		offset := value - s.base
		word := offset / 64
		return word < uint64(len(s.bitmap)) && s.bitmap[word]&(uint64(1)<<(offset%64)) != 0
	case ModeHash:
		_, ok := s.hash[value]
		return ok
	default:
		return false
	}
}

// Len returns the number of unique values.
func (s *Uint64Set) Len() int {
	if s == nil {
		return 0
	}
	return s.length
}

// Mode returns the selected representation.
func (s *Uint64Set) Mode() Mode {
	if s == nil {
		return ModeEmpty
	}
	return s.mode
}

// MemoryBytes estimates the representation backing bytes, excluding the
// caller's input and small fixed object header. Hash mode is an intentionally
// conservative estimate because map bucket overhead is runtime-dependent.
func (s *Uint64Set) MemoryBytes() uint64 {
	if s == nil {
		return 0
	}
	switch s.mode {
	case ModeSorted:
		return uint64(len(s.sorted)) * 8
	case ModeBitmap:
		return uint64(len(s.bitmap)) * 8
	case ModeHash:
		return uint64(len(s.hash)) * 16
	default:
		return 0
	}
}

func normalizeConfig(config Config) Config {
	defaults := DefaultConfig()
	if config.SortedMax == 0 {
		config.SortedMax = defaults.SortedMax
	}
	if config.BitmapSpan == 0 {
		config.BitmapSpan = defaults.BitmapSpan
	}
	if config.BitmapRatio == 0 {
		config.BitmapRatio = defaults.BitmapRatio
	}
	if config.HashMinSize == 0 {
		config.HashMinSize = defaults.HashMinSize
	}
	return config
}
