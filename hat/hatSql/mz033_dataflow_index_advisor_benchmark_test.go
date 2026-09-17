package hatSql

import (
	"sort"
	"sync"
	"testing"
)

type mz033ManualObservation struct {
	key              string
	field            string
	kind             string
	buildNanos       uint64
	probeNanos       uint64
	scanNanos        uint64
	maintenanceNanos uint64
	reads            uint64
	writes           uint64
	memoryBytes      uint64
}

type mz033ManualKey struct {
	key   string
	field string
	kind  string
}

type mz033ManualStats struct {
	candidate SQLArrangementCostCandidate
	samples   uint64
}

type mz033ManualAdvisor struct {
	mu       sync.Mutex
	capacity int
	minimum  uint64
	model    *SQLArrangementCostModel
	stats    map[mz033ManualKey]mz033ManualStats
}

var mz033DataflowBenchmarkSink uint64

func mz033ManualSaturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

func mz033BenchmarkObservations() []mz033ManualObservation {
	observations := make([]mz033ManualObservation, 10000)
	for index := range observations {
		candidate := uint64(index % 512)
		observations[index] = mz033ManualObservation{
			key:              "source_" + string(rune('a'+candidate/26)),
			field:            "field_" + string(rune('a'+candidate%26)),
			kind:             "hash",
			buildNanos:       400 + candidate%31,
			probeNanos:       20 + candidate%5,
			scanNanos:        320 + candidate%17,
			maintenanceNanos: 5 + candidate%3,
			reads:            4 + candidate%5,
			writes:           candidate % 2,
			memoryBytes:      256 + candidate%2048,
		}
	}
	return observations
}

func mz033SQLBenchmarkObservations() []SQLDataflowIndexObservation {
	manual := mz033BenchmarkObservations()
	observations := make([]SQLDataflowIndexObservation, len(manual))
	for index, observation := range manual {
		observations[index] = SQLDataflowIndexObservation{
			Candidate: SQLArrangementCostCandidate{
				Key:              observation.key,
				Field:            observation.field,
				Kind:             observation.kind,
				BuildCostNanos:   observation.buildNanos,
				ProbeCostNanos:   observation.probeNanos,
				ScanCostNanos:    observation.scanNanos,
				MaintenanceNanos: observation.maintenanceNanos,
				ExpectedReads:    observation.reads,
				ExpectedWrites:   observation.writes,
				MemoryBytes:      observation.memoryBytes,
			},
		}
	}
	return observations
}

func (advisor *mz033ManualAdvisor) observe(observation mz033ManualObservation) {
	advisor.mu.Lock()
	advisor.observeLocked(observation)
	advisor.mu.Unlock()
}

func (advisor *mz033ManualAdvisor) observeBatch(observations []mz033ManualObservation) int {
	advisor.mu.Lock()
	accepted := 0
	for _, observation := range observations {
		if advisor.observeLocked(observation) {
			accepted++
		}
	}
	advisor.mu.Unlock()
	return accepted
}

func (advisor *mz033ManualAdvisor) observeLocked(observation mz033ManualObservation) bool {
	key := mz033ManualKey{key: observation.key, field: observation.field, kind: observation.kind}
	stats, exists := advisor.stats[key]
	if !exists {
		if len(advisor.stats) >= advisor.capacity {
			return false
		}
		stats.candidate = SQLArrangementCostCandidate{Key: observation.key, Field: observation.field, Kind: observation.kind}
	}
	stats.samples = mz033ManualSaturatingAdd(stats.samples, 1)
	if observation.buildNanos > stats.candidate.BuildCostNanos {
		stats.candidate.BuildCostNanos = observation.buildNanos
	}
	if observation.probeNanos > stats.candidate.ProbeCostNanos {
		stats.candidate.ProbeCostNanos = observation.probeNanos
	}
	if observation.scanNanos > stats.candidate.ScanCostNanos {
		stats.candidate.ScanCostNanos = observation.scanNanos
	}
	if observation.maintenanceNanos > stats.candidate.MaintenanceNanos {
		stats.candidate.MaintenanceNanos = observation.maintenanceNanos
	}
	if observation.memoryBytes > stats.candidate.MemoryBytes {
		stats.candidate.MemoryBytes = observation.memoryBytes
	}
	stats.candidate.ExpectedReads = mz033ManualSaturatingAdd(stats.candidate.ExpectedReads, observation.reads)
	stats.candidate.ExpectedWrites = mz033ManualSaturatingAdd(stats.candidate.ExpectedWrites, observation.writes)
	advisor.stats[key] = stats
	return true
}

func (advisor *mz033ManualAdvisor) reset() {
	advisor.mu.Lock()
	clear(advisor.stats)
	advisor.mu.Unlock()
}

func (advisor *mz033ManualAdvisor) recommendations() []SQLArrangementCostScore {
	advisor.mu.Lock()
	scores := make([]SQLArrangementCostScore, 0, len(advisor.stats))
	for _, stats := range advisor.stats {
		if stats.samples < advisor.minimum {
			continue
		}
		score := advisor.model.Evaluate(stats.candidate)
		if score.Reusable {
			scores = append(scores, score)
		}
	}
	advisor.mu.Unlock()
	sort.SliceStable(scores, func(left, right int) bool {
		if scores[left].NetBenefitNanos != scores[right].NetBenefitNanos {
			return scores[left].NetBenefitNanos > scores[right].NetBenefitNanos
		}
		if scores[left].PaybackReads != scores[right].PaybackReads {
			return scores[left].PaybackReads < scores[right].PaybackReads
		}
		if scores[left].Candidate.Key != scores[right].Candidate.Key {
			return scores[left].Candidate.Key < scores[right].Candidate.Key
		}
		if scores[left].Candidate.Field != scores[right].Candidate.Field {
			return scores[left].Candidate.Field < scores[right].Candidate.Field
		}
		return scores[left].Candidate.Kind < scores[right].Candidate.Kind
	})
	return scores
}

func BenchmarkMZ033ManualDataflowRecommendation(b *testing.B) {
	observations := mz033BenchmarkObservations()
	model := NewSQLArrangementCostModel(SQLArrangementCostModelOptions{MemoryBudgetBytes: 4096})
	advisor := mz033ManualAdvisor{
		capacity: 512,
		minimum:  2,
		model:    model,
		stats:    make(map[mz033ManualKey]mz033ManualStats, 512),
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum uint64
	for iteration := 0; iteration < b.N; iteration++ {
		advisor.reset()
		if advisor.observeBatch(observations) != len(observations) {
			b.Fatal("benchmark batch observations were rejected")
		}
		for _, score := range advisor.recommendations() {
			checksum += uint64(score.NetBenefitNanos)
		}
	}
	mz033DataflowBenchmarkSink = checksum
}

func BenchmarkMZ033SQLDataflowIndexRecommendation(b *testing.B) {
	observations := mz033SQLBenchmarkObservations()
	options := SQLDataflowIndexAdvisorOptions{
		Capacity:   512,
		MinSamples: 2,
		CostModel:  SQLArrangementCostModelOptions{MemoryBudgetBytes: 4096},
	}
	advisor := NewSQLDataflowIndexAdvisor(options)
	b.ReportAllocs()
	b.ResetTimer()
	var checksum uint64
	for iteration := 0; iteration < b.N; iteration++ {
		advisor.Reset()
		if advisor.ObserveDataflows(observations) != len(observations) {
			b.Fatal("benchmark batch observations were rejected")
		}
		for _, recommendation := range advisor.Recommendations(0) {
			checksum += uint64(recommendation.NetBenefitNanos)
		}
	}
	mz033DataflowBenchmarkSink = checksum
}
