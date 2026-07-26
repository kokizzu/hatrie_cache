package hatriecache

import (
	"fmt"
	"io"
	"testing"
	"time"

	json "github.com/goccy/go-json"
)

func BenchmarkStructuredValidationEncoder(b *testing.B) {
	for _, size := range []int{64, 4096} {
		for _, nested := range []bool{false, true} {
			name := fmt.Sprintf("Scalar%d", size)
			if nested {
				name = fmt.Sprintf("Nested%d", size)
			}
			b.Run(name, func(b *testing.B) {
				values := commandFieldsBenchmarkPairs(size)
				if nested {
					values["nested"] = Map{"field": "value"}
				}
				b.Run("Marshal", func(b *testing.B) {
					b.ReportAllocs()
					for iteration := 0; iteration < b.N; iteration++ {
						if _, err := json.Marshal(values); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("Encoder", func(b *testing.B) {
					b.ReportAllocs()
					for iteration := 0; iteration < b.N; iteration++ {
						if err := json.NewEncoder(io.Discard).Encode(values); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}

func BenchmarkFlatScalarStructuredValidationAlternating(b *testing.B) {
	for _, size := range []int{64, 4096} {
		b.Run(fmt.Sprintf("Fields%d", size), func(b *testing.B) {
			values := commandFieldsBenchmarkPairs(size)
			validations := 4096
			if size >= 4096 {
				validations = 64
			}
			var scalarDuration, marshalDuration time.Duration
			for iteration := 0; iteration < b.N; iteration++ {
				scalarFirst := iteration&1 != 0
				for pass := 0; pass < 2; pass++ {
					started := time.Now()
					if scalarFirst == (pass == 0) {
						for validation := 0; validation < validations; validation++ {
							if !flatJSONScalarMap(values) {
								b.Fatal("scalar validation failed")
							}
						}
						scalarDuration += time.Since(started)
					} else {
						for validation := 0; validation < validations; validation++ {
							if _, err := json.Marshal(values); err != nil {
								b.Fatal(err)
							}
						}
						marshalDuration += time.Since(started)
					}
				}
			}
			operations := float64(b.N * validations)
			b.ReportMetric(float64(marshalDuration.Nanoseconds())/operations, "marshal-ns/validation")
			b.ReportMetric(float64(scalarDuration.Nanoseconds())/operations, "scalar-ns/validation")
		})
	}
}

func BenchmarkStructuredValidationFallbackAlternating(b *testing.B) {
	for _, size := range []int{64, 4096} {
		b.Run(fmt.Sprintf("Fields%d", size), func(b *testing.B) {
			values := commandFieldsBenchmarkPairs(size)
			values["nested"] = Map{"field": "value"}
			validations := 4096
			if size >= 4096 {
				validations = 64
			}
			var candidateDuration, controlDuration time.Duration
			for iteration := 0; iteration < b.N; iteration++ {
				candidateFirst := iteration&1 != 0
				for pass := 0; pass < 2; pass++ {
					started := time.Now()
					if candidateFirst == (pass == 0) {
						for validation := 0; validation < validations; validation++ {
							if err := validateMapValue(values); err != nil {
								b.Fatal(err)
							}
						}
						candidateDuration += time.Since(started)
					} else {
						for validation := 0; validation < validations; validation++ {
							if _, err := json.Marshal(values); err != nil {
								b.Fatal(err)
							}
						}
						controlDuration += time.Since(started)
					}
				}
			}
			operations := float64(b.N * validations)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/validation")
			b.ReportMetric(float64(controlDuration.Nanoseconds())/operations, "control-ns/validation")
		})
	}
}
