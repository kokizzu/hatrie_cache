package hatCodec_test

import (
	"errors"
	"math"
	"testing"

	hatCodec "hatrie_cache/hat/hatCodec"
)

func TestSelectCodecFromSampleUsesEntropyRecommendation(t *testing.T) {
	repeated := make([]byte, 1024)
	choice, entropy, err := hatCodec.SelectCodecFromSample(repeated)
	if err != nil {
		t.Fatalf("SelectCodecFromSample(repeated) error = %v", err)
	}
	if choice != hatCodec.CodecChoiceCompressedBlocks || entropy != 0 {
		t.Fatalf("repeated sample = choice %q entropy %v", choice, entropy)
	}

	uniform := make([]byte, 1024)
	for index := range uniform {
		uniform[index] = byte(index)
	}
	choice, entropy, err = hatCodec.SelectCodecFromSample(uniform)
	if err != nil {
		t.Fatalf("SelectCodecFromSample(uniform) error = %v", err)
	}
	if choice != hatCodec.CodecChoiceRaw || math.Abs(entropy-8) > 1e-12 {
		t.Fatalf("uniform sample = choice %q entropy %v", choice, entropy)
	}
}

func TestSelectCodecFromEntropyRejectsInvalidValues(t *testing.T) {
	for name, entropy := range map[string]float64{
		"negative":           -0.1,
		"above byte maximum": 8.1,
		"nan":                math.NaN(),
		"positive infinity":  math.Inf(1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := hatCodec.SelectCodecFromEntropy(entropy); !errors.Is(err, hatCodec.ErrCodecSelectionInvalid) {
				t.Fatalf("SelectCodecFromEntropy(%v) error = %v, want ErrCodecSelectionInvalid", entropy, err)
			}
		})
	}
}

func TestSelectCodecFromSampleAcceptsEmptyInput(t *testing.T) {
	choice, entropy, err := hatCodec.SelectCodecFromSample(nil)
	if err != nil {
		t.Fatalf("SelectCodecFromSample(empty) error = %v", err)
	}
	if choice != hatCodec.CodecChoiceCompressedBlocks || entropy != 0 {
		t.Fatalf("empty sample = choice %q entropy %v", choice, entropy)
	}
}

func BenchmarkEstimateByteEntropy(b *testing.B) {
	sample := make([]byte, 4096)
	for index := range sample {
		sample[index] = byte(index)
	}
	b.ReportAllocs()
	for range b.N {
		_ = hatCodec.EstimateByteEntropy(sample)
	}
}
