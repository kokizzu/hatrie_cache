package hatriecache

import (
	"fmt"
	"testing"
	"time"

	json "github.com/goccy/go-json"
)

func BenchmarkSequenceValidation(b *testing.B) {
	for _, size := range []int{64, 4096} {
		values := commandFieldsBenchmarkValues(size)
		queue := make(PriorityQueue, size)
		for index, value := range values {
			queue[index] = PriorityItem{Priority: int64(index), Value: value}
		}
		b.Run(fmt.Sprintf("Slice%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := validateSliceValue(values); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("SlicePayload%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := validateSliceValues(values[0], values[1:]...); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("PriorityQueue%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := validatePriorityQueueValue(queue); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("PriorityPayload%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := validatePriorityQueuePayload(values[0], values[1:]...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSequenceCheckedReplacement(b *testing.B) {
	for _, size := range []int{64, 4096} {
		values := commandFieldsBenchmarkValues(size)
		queue := make(PriorityQueue, size)
		for index, value := range values {
			queue[index] = PriorityItem{Priority: int64(index), Value: value}
		}
		b.Run(fmt.Sprintf("Slice%d", size), func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			if err := trie.UpsertSliceChecked("sequence", values); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := trie.UpsertSliceChecked("sequence", values); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("PriorityQueue%d", size), func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			if err := trie.UpsertPriorityQueueChecked("sequence", queue); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := trie.UpsertPriorityQueueChecked("sequence", queue); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSequenceValidationFallbackAlternating(b *testing.B) {
	for _, size := range []int{64, 4096} {
		values := commandFieldsBenchmarkValues(size)
		values[len(values)-1] = Map{"field": "value"}
		queue := make(PriorityQueue, size)
		for index, value := range values {
			queue[index] = PriorityItem{Priority: int64(index), Value: value}
		}
		validations := 4096
		if size >= 4096 {
			validations = 64
		}
		for _, benchmark := range []struct {
			name      string
			candidate func() error
			control   func() error
		}{
			{
				name:      fmt.Sprintf("Slice%d", size),
				candidate: func() error { return validateSliceValue(values) },
				control: func() error {
					_, err := json.Marshal(values)
					return err
				},
			},
			{
				name:      fmt.Sprintf("PriorityQueue%d", size),
				candidate: func() error { return validatePriorityQueueValue(queue) },
				control: func() error {
					_, err := json.Marshal(queue)
					return err
				},
			},
		} {
			b.Run(benchmark.name, func(b *testing.B) {
				var candidateDuration, controlDuration time.Duration
				for iteration := 0; iteration < b.N; iteration++ {
					candidateFirst := iteration&1 != 0
					for pass := 0; pass < 2; pass++ {
						started := time.Now()
						validate := benchmark.control
						if candidateFirst == (pass == 0) {
							validate = benchmark.candidate
						}
						for validation := 0; validation < validations; validation++ {
							if err := validate(); err != nil {
								b.Fatal(err)
							}
						}
						duration := time.Since(started)
						if candidateFirst == (pass == 0) {
							candidateDuration += duration
						} else {
							controlDuration += duration
						}
					}
				}
				operations := float64(b.N * validations)
				b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/validation")
				b.ReportMetric(float64(controlDuration.Nanoseconds())/operations, "control-ns/validation")
			})
		}
	}
}

func BenchmarkSequenceValidationFallbackAllocations(b *testing.B) {
	for _, size := range []int{64, 4096} {
		values := commandFieldsBenchmarkValues(size)
		values[len(values)-1] = Map{"field": "value"}
		queue := make(PriorityQueue, size)
		for index, value := range values {
			queue[index] = PriorityItem{Priority: int64(index), Value: value}
		}
		for _, benchmark := range []struct {
			name      string
			candidate func() error
			control   func() error
		}{
			{
				name:      fmt.Sprintf("Slice%d", size),
				candidate: func() error { return validateSliceValue(values) },
				control: func() error {
					_, err := json.Marshal(values)
					return err
				},
			},
			{
				name:      fmt.Sprintf("PriorityQueue%d", size),
				candidate: func() error { return validatePriorityQueueValue(queue) },
				control: func() error {
					_, err := json.Marshal(queue)
					return err
				},
			},
		} {
			b.Run(benchmark.name, func(b *testing.B) {
				for _, validation := range []struct {
					name string
					run  func() error
				}{
					{name: "Control", run: benchmark.control},
					{name: "Candidate", run: benchmark.candidate},
				} {
					b.Run(validation.name, func(b *testing.B) {
						b.ReportAllocs()
						for iteration := 0; iteration < b.N; iteration++ {
							if err := validation.run(); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			})
		}
	}
}

func commandFieldsBenchmarkValues(count int) Slice {
	values := make(Slice, count)
	for index := range values {
		values[index] = fmt.Sprintf("value:%06d", index)
	}
	return values
}
