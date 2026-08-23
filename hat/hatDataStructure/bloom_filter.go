package hatDataStructure

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	mathbits "math/bits"

	"hatrie_cache/hat/hatHash"
)

// BloomFilterInfo reports the shape and current fill level of a Bloom filter.
// Bloom filters do not store their inserted values, only a compact bitset.
type BloomFilterInfo struct {
	BitCount                   uint64  `json:"bit_count"`
	BitBytes                   uint64  `json:"bit_bytes"`
	HashCount                  uint8   `json:"hash_count"`
	Insertions                 uint64  `json:"insertions"`
	SetBits                    uint64  `json:"set_bits"`
	FillRatio                  float64 `json:"fill_ratio"`
	EstimatedFalsePositiveRate float64 `json:"estimated_false_positive_rate"`
}

// BloomFilterSnapshot is the portable serialized form of a Bloom filter.
type BloomFilterSnapshot struct {
	BitCount   uint64 `json:"bit_count"`
	HashCount  uint8  `json:"hash_count"`
	Insertions uint64 `json:"insertions"`
	Bits       string `json:"bits"`
}

// BloomFilter stores a compact probabilistic membership bitset. Its backing
// words are allocated lazily, so an empty configured filter has no bit payload.
type BloomFilter struct {
	words      []uint64
	insertions uint64
	bitCount   uint32
	hashCount  uint8
}

// NewBloomFilter creates a filter sized for expectedItems at falsePositiveRate.
func NewBloomFilter(expectedItems uint64, falsePositiveRate float64) (BloomFilter, error) {
	bitCount, hashCount, err := BloomFilterShape(expectedItems, falsePositiveRate)
	if err != nil {
		return BloomFilter{}, err
	}
	return NewBloomFilterWithShape(bitCount, hashCount)
}

// NewBloomFilterWithShape creates a filter with an explicit bit and hash count.
func NewBloomFilterWithShape(bitCount uint64, hashCount uint8) (BloomFilter, error) {
	if bitCount < MinBloomFilterBits || bitCount > MaxBloomFilterBits {
		return BloomFilter{}, errors.New("hatriecache: invalid bloom filter bit count")
	}
	if hashCount == 0 || hashCount > MaxBloomFilterHashes {
		return BloomFilter{}, errors.New("hatriecache: invalid bloom filter hash count")
	}
	return BloomFilter{bitCount: uint32(bitCount), hashCount: hashCount}, nil
}

// ValidateBloomFilterSnapshot validates a portable Bloom filter snapshot.
func ValidateBloomFilterSnapshot(snapshot BloomFilterSnapshot) error {
	if snapshot.BitCount < MinBloomFilterBits || snapshot.BitCount > MaxBloomFilterBits {
		return errors.New("hatriecache: invalid bloom filter bit count")
	}
	if snapshot.HashCount == 0 || snapshot.HashCount > MaxBloomFilterHashes {
		return errors.New("hatriecache: invalid bloom filter hash count")
	}
	size, ok := bloomFilterBase64DecodedSize(snapshot.Bits)
	if !ok {
		return errors.New("hatriecache: invalid base64 encoding")
	}
	if size == 0 {
		if snapshot.Insertions != 0 {
			return errors.New("hatriecache: empty bloom filter bitset has insertions")
		}
		return nil
	}
	if uint64(size) != BloomFilterWordCount(snapshot.BitCount)*8 {
		return errors.New("hatriecache: invalid bloom filter bitset length")
	}
	data, err := base64.StdEncoding.DecodeString(snapshot.Bits)
	if err != nil {
		return err
	}
	if err := ValidateBloomFilterUnusedBits(snapshot.BitCount, data); err != nil {
		return err
	}
	setBits := BloomFilterRawSetBits(data)
	if snapshot.Insertions == 0 && setBits != 0 {
		return errors.New("hatriecache: empty bloom filter snapshot has set bits")
	}
	if snapshot.Insertions > 0 && setBits == 0 {
		return errors.New("hatriecache: populated bloom filter snapshot has no set bits")
	}
	return nil
}

// NewBloomFilterFromSnapshot restores a validated portable Bloom filter snapshot.
func NewBloomFilterFromSnapshot(snapshot BloomFilterSnapshot) (BloomFilter, error) {
	if err := ValidateBloomFilterSnapshot(snapshot); err != nil {
		return BloomFilter{}, err
	}
	raw, err := base64.StdEncoding.DecodeString(snapshot.Bits)
	if err != nil {
		return BloomFilter{}, err
	}
	filter := BloomFilter{bitCount: uint32(snapshot.BitCount), hashCount: snapshot.HashCount, insertions: snapshot.Insertions}
	if len(raw) == 0 || (snapshot.Insertions == 0 && BloomFilterRawSetBits(raw) == 0) {
		return filter, nil
	}
	filter.words = make([]uint64, len(raw)/8)
	for index := range filter.words {
		filter.words[index] = binary.LittleEndian.Uint64(raw[index*8 : index*8+8])
	}
	filter.maskUnusedBits()
	return filter, nil
}

// AddBytes records a byte payload and reports whether it changed any bit.
func (filter *BloomFilter) AddBytes(value []byte) bool {
	if filter == nil || filter.bitCount == 0 || filter.hashCount == 0 {
		return false
	}
	return filter.addHashed(hatHash.FNV1a64(value), hatHash.FNV1_64(value))
}

// AddJSONString records an already canonical JSON string without allocating its
// encoded byte representation, and reports whether it changed any bit.
func (filter *BloomFilter) AddJSONString(value string) bool {
	if filter == nil || filter.bitCount == 0 || filter.hashCount == 0 {
		return false
	}
	return filter.addHashed(hatHash.FNV1a64JSONString(value), hatHash.FNV1_64JSONString(value))
}

// ContainsBytes reports whether a byte payload may have been recorded.
func (filter *BloomFilter) ContainsBytes(value []byte) bool {
	if filter == nil || len(filter.words) == 0 {
		return false
	}
	return filter.containsHashed(hatHash.FNV1a64(value), hatHash.FNV1_64(value))
}

// ContainsJSONString reports whether an already canonical JSON string may have
// been recorded without allocating its encoded byte representation.
func (filter *BloomFilter) ContainsJSONString(value string) bool {
	if filter == nil || len(filter.words) == 0 {
		return false
	}
	return filter.containsHashed(hatHash.FNV1a64JSONString(value), hatHash.FNV1_64JSONString(value))
}

func (filter *BloomFilter) addHashed(first, step uint64) bool {
	filter.ensureWords()
	step = bloomFilterHashStep(step)
	changed := false
	for index := uint8(0); index < filter.hashCount; index++ {
		bit := (first + uint64(index)*step) % uint64(filter.bitCount)
		word, mask := bit/64, uint64(1)<<uint(bit%64)
		if filter.words[word]&mask == 0 {
			filter.words[word] |= mask
			changed = true
		}
	}
	if changed {
		filter.insertions++
	}
	return changed
}

func (filter *BloomFilter) containsHashed(first, step uint64) bool {
	step = bloomFilterHashStep(step)
	for index := uint8(0); index < filter.hashCount; index++ {
		bit := (first + uint64(index)*step) % uint64(filter.bitCount)
		if filter.words[bit/64]&(uint64(1)<<uint(bit%64)) == 0 {
			return false
		}
	}
	return true
}

func bloomFilterHashStep(step uint64) uint64 {
	if step == 0 {
		step = hatHash.FNVPrime64
	}
	return step | 1
}

// Info reports the shape and current fill level.
func (filter BloomFilter) Info() BloomFilterInfo {
	setBits := filter.SetBits()
	fillRatio := 0.0
	if filter.bitCount > 0 {
		fillRatio = float64(setBits) / float64(filter.bitCount)
	}
	return BloomFilterInfo{
		BitCount: uint64(filter.bitCount), BitBytes: uint64(len(filter.words)) * 8,
		HashCount: filter.hashCount, Insertions: filter.insertions, SetBits: setBits,
		FillRatio: fillRatio, EstimatedFalsePositiveRate: math.Pow(fillRatio, float64(filter.hashCount)),
	}
}

// SetBits returns the number of populated bits.
func (filter BloomFilter) SetBits() uint64 {
	var total uint64
	for _, word := range filter.words {
		total += uint64(mathbits.OnesCount64(word))
	}
	return total
}

// Snapshot returns the portable serialized form.
func (filter BloomFilter) Snapshot() BloomFilterSnapshot {
	var data []byte
	if len(filter.words) > 0 {
		data = make([]byte, len(filter.words)*8)
		for index, word := range filter.words {
			binary.LittleEndian.PutUint64(data[index*8:index*8+8], word)
		}
	}
	return BloomFilterSnapshot{BitCount: uint64(filter.bitCount), HashCount: filter.hashCount, Insertions: filter.insertions, Bits: base64.StdEncoding.EncodeToString(data)}
}

// EncodedSize returns the byte size of the populated backing bitset.
func (filter BloomFilter) EncodedSize() int64 { return int64(len(filter.words) * 8) }

// BitCount returns the configured number of usable bits.
func (filter BloomFilter) BitCount() uint64 { return uint64(filter.bitCount) }

// HashCount returns the configured hash count.
func (filter BloomFilter) HashCount() uint8 { return filter.hashCount }

// Insertions returns the number of additions that changed at least one bit.
func (filter BloomFilter) Insertions() uint64 { return filter.insertions }

// RawWords returns the backing words without copying. Callers must not retain or
// mutate the returned slice.
func (filter BloomFilter) RawWords() []uint64 { return filter.words }

func (filter *BloomFilter) ensureWords() {
	if filter != nil && len(filter.words) == 0 && filter.bitCount > 0 {
		filter.words = make([]uint64, int(BloomFilterWordCount(uint64(filter.bitCount))))
	}
}

func (filter *BloomFilter) maskUnusedBits() {
	if filter == nil || len(filter.words) == 0 || filter.bitCount%64 == 0 {
		return
	}
	filter.words[len(filter.words)-1] &= (uint64(1) << uint(filter.bitCount%64)) - 1
}

func bloomFilterBase64DecodedSize(value string) (int, bool) {
	if len(value)%4 != 0 {
		return 0, false
	}
	padding := 0
	if len(value) >= 2 && value[len(value)-2:] == "==" {
		padding = 2
	} else if len(value) >= 1 && value[len(value)-1:] == "=" {
		padding = 1
	}
	return len(value)/4*3 - padding, true
}
