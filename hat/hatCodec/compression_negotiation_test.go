package hatCodec_test

import (
	"compress/flate"
	"errors"
	"testing"

	"hatrie_cache/hat/hatCodec"
)

func TestCompressionLevelPolicyNegotiatesWithinBothRanges(t *testing.T) {
	policy, err := hatCodec.NewCompressionLevelPolicy(flate.BestCompression, flate.HuffmanOnly, flate.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	preferred := flate.BestCompression
	for _, test := range []struct {
		name      string
		clientMin int
		clientMax int
		preferred *int
		want      int
	}{
		{name: "preferred allowed", clientMin: flate.HuffmanOnly, clientMax: flate.BestCompression, preferred: &preferred, want: flate.BestCompression},
		{name: "preferred clamped high", clientMin: flate.HuffmanOnly, clientMax: flate.BestSpeed, preferred: &preferred, want: flate.BestSpeed},
		{name: "preferred clamped low", clientMin: flate.BestCompression, clientMax: flate.BestCompression, preferred: &preferred, want: flate.BestCompression},
		{name: "default used", clientMin: flate.HuffmanOnly, clientMax: flate.BestCompression, want: flate.BestCompression},
		{name: "default clamped low", clientMin: flate.HuffmanOnly, clientMax: flate.BestSpeed, want: flate.BestSpeed},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := policy.Negotiate(test.clientMin, test.clientMax, test.preferred)
			if err != nil || got != test.want {
				t.Fatalf("Negotiate() = %d, %v, want %d", got, err, test.want)
			}
		})
	}
}

func TestCompressionLevelPolicyRejectsInvalidRanges(t *testing.T) {
	for index, levels := range [][3]int{
		{flate.BestSpeed, flate.BestCompression, flate.HuffmanOnly},
		{flate.HuffmanOnly - 1, flate.HuffmanOnly, flate.BestCompression},
		{flate.BestCompression + 1, flate.HuffmanOnly, flate.BestCompression},
	} {
		if _, err := hatCodec.NewCompressionLevelPolicy(levels[0], levels[1], levels[2]); !errors.Is(err, hatCodec.ErrCompressionLevelPolicyInvalid) {
			t.Errorf("constructor case %d error = %v", index, err)
		}
	}
	policy, err := hatCodec.NewCompressionLevelPolicy(flate.BestSpeed, flate.HuffmanOnly, flate.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	for index, bounds := range [][2]int{
		{flate.BestCompression, flate.BestSpeed},
		{flate.HuffmanOnly - 1, flate.BestCompression},
		{flate.HuffmanOnly, flate.BestCompression + 1},
	} {
		if _, err := policy.Negotiate(bounds[0], bounds[1], nil); !errors.Is(err, hatCodec.ErrCompressionLevelPolicyInvalid) {
			t.Errorf("client case %d error = %v", index, err)
		}
	}
}

func BenchmarkCompressionLevelPolicyNegotiate(b *testing.B) {
	policy, err := hatCodec.NewCompressionLevelPolicy(flate.BestSpeed, flate.HuffmanOnly, flate.BestCompression)
	if err != nil {
		b.Fatal(err)
	}
	preferred := flate.BestCompression
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := policy.Negotiate(flate.HuffmanOnly, flate.BestCompression, &preferred); err != nil {
			b.Fatal(err)
		}
	}
}
