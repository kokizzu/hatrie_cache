package hatCache

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

func BenchmarkWholeSequenceSparseFallback(b *testing.B) {
	for _, size := range []int{64, 4096} {
		values, queue := sparseSequenceBenchmarkValues(size, 1)
		for _, validation := range []struct {
			name string
			run  func() error
		}{
			{name: "Slice", run: func() error { return validateSliceValue(values) }},
			{name: "PriorityQueue", run: func() error { return validatePriorityQueueValue(queue) }},
		} {
			b.Run(fmt.Sprintf("%s%d", validation.name, size), func(b *testing.B) {
				b.ReportAllocs()
				for iteration := 0; iteration < b.N; iteration++ {
					if err := validation.run(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkWholeSequenceSparseCheckedReplacement(b *testing.B) {
	for _, size := range []int{64, 4096} {
		values, queue := sparseSequenceBenchmarkValues(size, 1)
		for _, replacement := range []struct {
			name string
			run  func(*HatTrie) error
		}{
			{name: "Slice", run: func(trie *HatTrie) error { return trie.UpsertSliceChecked("sequence", values) }},
			{name: "PriorityQueue", run: func(trie *HatTrie) error {
				return trie.UpsertPriorityQueueChecked("sequence", queue)
			}},
		} {
			b.Run(fmt.Sprintf("%s%d", replacement.name, size), func(b *testing.B) {
				trie := CreateHatTrie()
				b.Cleanup(trie.Destroy)
				if err := replacement.run(trie); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if err := replacement.run(trie); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func validateSliceValueWholeControl(value Slice) error {
	if flatJSONScalarSlice(value) {
		return nil
	}
	return validateJSONToDiscard(value)
}

func validatePriorityQueueValueWholeControl(value PriorityQueue) error {
	for _, item := range value {
		if !flatJSONScalar(item.Value) {
			return validateJSONToDiscard(value)
		}
	}
	return nil
}

func BenchmarkWholeSequenceFallbackAlternating(b *testing.B) {
	for _, nested := range []int{1, 2} {
		for _, size := range []int{64, 4096} {
			values, queue := sparseSequenceBenchmarkValues(size, nested)
			validations := 4096
			if size >= 4096 {
				validations = 64
			}
			for _, validation := range []struct {
				name      string
				candidate func() error
				control   func() error
			}{
				{
					name:      "Slice",
					candidate: func() error { return validateSliceValue(values) },
					control:   func() error { return validateSliceValueWholeControl(values) },
				},
				{
					name:      "PriorityQueue",
					candidate: func() error { return validatePriorityQueueValue(queue) },
					control:   func() error { return validatePriorityQueueValueWholeControl(queue) },
				},
			} {
				b.Run(fmt.Sprintf("Nested%d/%s%d", nested, validation.name, size), func(b *testing.B) {
					var candidateDuration, controlDuration time.Duration
					for iteration := 0; iteration < b.N; iteration++ {
						candidateFirst := iteration&1 != 0
						for pass := 0; pass < 2; pass++ {
							started := time.Now()
							run := validation.control
							if candidateFirst == (pass == 0) {
								run = validation.candidate
							}
							for validationIndex := 0; validationIndex < validations; validationIndex++ {
								if err := run(); err != nil {
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
}

func BenchmarkWholeSequenceFallbackAllocations(b *testing.B) {
	for _, size := range []int{64, 4096} {
		values, queue := sparseSequenceBenchmarkValues(size, 2)
		for _, validation := range []struct {
			name      string
			candidate func() error
			control   func() error
		}{
			{
				name:      "Slice",
				candidate: func() error { return validateSliceValue(values) },
				control:   func() error { return validateSliceValueWholeControl(values) },
			},
			{
				name:      "PriorityQueue",
				candidate: func() error { return validatePriorityQueueValue(queue) },
				control:   func() error { return validatePriorityQueueValueWholeControl(queue) },
			},
		} {
			for _, implementation := range []struct {
				name string
				run  func() error
			}{
				{name: "Control", run: validation.control},
				{name: "Candidate", run: validation.candidate},
			} {
				b.Run(fmt.Sprintf("%s%d/%s", validation.name, size, implementation.name), func(b *testing.B) {
					b.ReportAllocs()
					for iteration := 0; iteration < b.N; iteration++ {
						if err := implementation.run(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func sparseSequenceBenchmarkValues(size int, nested int) (Slice, PriorityQueue) {
	values := commandFieldsBenchmarkValues(size)
	queue := make(PriorityQueue, size)
	for index := 0; index < nested; index++ {
		values[len(values)-1-index] = Map{"field": "value"}
	}
	for index, value := range values {
		queue[index] = PriorityItem{Priority: int64(index), Value: value}
	}
	return values, queue
}

func BenchmarkSlicePayloadSparseFallback(b *testing.B) {
	for _, size := range []int{64, 4096} {
		values := commandFieldsBenchmarkValues(size)
		values[len(values)-1] = Map{"field": "value"}
		b.Run(fmt.Sprintf("Items%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := validateSliceValues(values[0], values[1:]...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSliceCheckedSparsePush(b *testing.B) {
	for _, size := range []int{64, 4096} {
		values := commandFieldsBenchmarkValues(size)
		values[len(values)-1] = Map{"field": "value"}
		b.Run(fmt.Sprintf("Items%d", size), func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				if err := trie.PushSliceChecked("sequence", values[0], values[1:]...); err != nil {
					b.Fatal(err)
				}
				if !trie.Delete("sequence") {
					b.Fatal("Delete(sequence) = false")
				}
			}
		})
	}
}

func validateSliceValuesMaterializedControl(value interface{}, values ...interface{}) error {
	capacity, ok := checkedBatchSize(1, len(values))
	if !ok {
		return errBatchSizeTooLarge
	}
	if flatJSONScalar(value) {
		allScalars := true
		for _, next := range values {
			if !flatJSONScalar(next) {
				allScalars = false
				break
			}
		}
		if allScalars {
			return nil
		}
	}
	items := make(Slice, 0, capacity)
	items = append(items, value)
	items = append(items, values...)
	return validateJSONToDiscard(items)
}

func BenchmarkSparseSlicePayloadFallbackAlternating(b *testing.B) {
	benchmarkSlicePayloadFallbackAlternating(b, 1)
}

func BenchmarkMultipleSlicePayloadFallbackAlternating(b *testing.B) {
	benchmarkSlicePayloadFallbackAlternating(b, 2)
}

func benchmarkSlicePayloadFallbackAlternating(b *testing.B, nested int) {
	for _, size := range []int{64, 4096} {
		values := commandFieldsBenchmarkValues(size)
		for index := 0; index < nested; index++ {
			values[len(values)-1-index] = Map{"field": "value"}
		}
		validations := 4096
		if size >= 4096 {
			validations = 64
		}
		b.Run(fmt.Sprintf("Items%d", size), func(b *testing.B) {
			var candidateDuration, controlDuration time.Duration
			for iteration := 0; iteration < b.N; iteration++ {
				candidateFirst := iteration&1 != 0
				for pass := 0; pass < 2; pass++ {
					started := time.Now()
					for validation := 0; validation < validations; validation++ {
						var err error
						if candidateFirst == (pass == 0) {
							err = validateSliceValues(values[0], values[1:]...)
						} else {
							err = validateSliceValuesMaterializedControl(values[0], values[1:]...)
						}
						if err != nil {
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

func BenchmarkSlicePayloadFallbackAllocations(b *testing.B) {
	for _, size := range []int{64, 4096} {
		for _, fallback := range []struct {
			name   string
			nested int
		}{
			{name: "Single", nested: 1},
			{name: "Multiple", nested: 2},
		} {
			values := commandFieldsBenchmarkValues(size)
			for index := 0; index < fallback.nested; index++ {
				values[len(values)-1-index] = Map{"field": "value"}
			}
			b.Run(fmt.Sprintf("%s/Items%d", fallback.name, size), func(b *testing.B) {
				for _, validation := range []struct {
					name string
					run  func(interface{}, ...interface{}) error
				}{
					{name: "Control", run: validateSliceValuesMaterializedControl},
					{name: "Candidate", run: validateSliceValues},
				} {
					b.Run(validation.name, func(b *testing.B) {
						b.ReportAllocs()
						for iteration := 0; iteration < b.N; iteration++ {
							if err := validation.run(values[0], values[1:]...); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			})
		}
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
