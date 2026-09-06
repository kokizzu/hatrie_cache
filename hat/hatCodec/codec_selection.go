package hatCodec

import (
	"errors"
	"math"
)

var ErrCodecSelectionInvalid = errors.New("hatriecache: codec selection entropy is invalid")

// CodecChoice is an advisory representation choice for sampled data.
type CodecChoice string

const (
	CodecChoiceCompressedBlocks CodecChoice = "compressed_blocks"
	CodecChoiceRaw              CodecChoice = "raw"
	// RawEntropyThreshold selects raw storage at or above this bits-per-byte
	// estimate, where compression is unlikely to repay its CPU cost.
	RawEntropyThreshold = 7.0
)

// EstimateByteEntropy returns the Shannon entropy estimate in bits per byte.
// The 256-bin histogram stays on the stack and the input is not retained.
func EstimateByteEntropy(sample []byte) float64 {
	if len(sample) == 0 {
		return 0
	}
	var counts [256]uint64
	for _, value := range sample {
		counts[value]++
	}
	length := float64(len(sample))
	entropy := 0.0
	for _, count := range counts {
		if count == 0 {
			continue
		}
		probability := float64(count) / length
		entropy -= probability * math.Log2(probability)
	}
	return entropy
}

// SelectCodecFromEntropy returns a conservative representation recommendation
// from an entropy estimate. It does not perform compression or change a
// caller's configured codec.
func SelectCodecFromEntropy(entropy float64) (CodecChoice, error) {
	if math.IsNaN(entropy) || math.IsInf(entropy, 0) || entropy < 0 || entropy > 8 {
		return "", ErrCodecSelectionInvalid
	}
	if entropy >= RawEntropyThreshold {
		return CodecChoiceRaw, nil
	}
	return CodecChoiceCompressedBlocks, nil
}

// SelectCodecFromSample estimates entropy and returns the corresponding
// representation recommendation.
func SelectCodecFromSample(sample []byte) (CodecChoice, float64, error) {
	entropy := EstimateByteEntropy(sample)
	choice, err := SelectCodecFromEntropy(entropy)
	return choice, entropy, err
}
