package hatSql

import "testing"

var mz032BenchmarkSink uint64

func mz032BenchmarkCandidates() []SQLArrangementCostCandidate {
	candidates := make([]SQLArrangementCostCandidate, 10000)
	for index := range candidates {
		value := uint64(index)
		candidates[index] = SQLArrangementCostCandidate{
			BuildCostNanos:   1200 + value%97,
			ProbeCostNanos:   80 + value%13,
			ScanCostNanos:    420 + value%53,
			MaintenanceNanos: 15 + value%7,
			ExpectedReads:    50 + value%211,
			ExpectedWrites:   value % 19,
			MemoryBytes:      256 + value%4096,
		}
	}
	return candidates
}

func mz032ManualSaturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

func mz032ManualSaturatingMul(left, right uint64) uint64 {
	if left == 0 || right == 0 {
		return 0
	}
	if left > ^uint64(0)/right {
		return ^uint64(0)
	}
	return left * right
}

func mz032ManualNetBenefit(benefit, cost uint64) int64 {
	if benefit >= cost {
		difference := benefit - cost
		if difference > uint64(1<<63-1) {
			return int64(1<<63 - 1)
		}
		return int64(difference)
	}
	difference := cost - benefit
	if difference >= uint64(1<<63) {
		return -1 << 63
	}
	return -int64(difference)
}

func mz032ManualArrangementScore(candidate SQLArrangementCostCandidate) SQLArrangementCostScore {
	readSavings := uint64(0)
	if candidate.ScanCostNanos > candidate.ProbeCostNanos {
		readSavings = candidate.ScanCostNanos - candidate.ProbeCostNanos
	}
	totalCost := mz032ManualSaturatingAdd(candidate.BuildCostNanos, mz032ManualSaturatingMul(candidate.MaintenanceNanos, candidate.ExpectedWrites))
	expectedBenefit := mz032ManualSaturatingMul(readSavings, candidate.ExpectedReads)
	netBenefit := mz032ManualNetBenefit(expectedBenefit, totalCost)
	paybackReads := uint64(0)
	if readSavings > 0 && totalCost > 0 {
		paybackReads = (totalCost-1)/readSavings + 1
	}
	return SQLArrangementCostScore{
		Candidate:            candidate,
		ReadSavingsNanos:     readSavings,
		TotalCostNanos:       totalCost,
		ExpectedBenefitNanos: expectedBenefit,
		NetBenefitNanos:      netBenefit,
		PaybackReads:         paybackReads,
		FitsMemory:           true,
		Reusable:             readSavings > 0 && paybackReads <= candidate.ExpectedReads && netBenefit > 0,
	}
}

func BenchmarkMZ032ManualArrangementCost(b *testing.B) {
	candidates := mz032BenchmarkCandidates()
	b.ReportAllocs()
	b.ResetTimer()
	var checksum uint64
	for iteration := 0; iteration < b.N; iteration++ {
		for _, candidate := range candidates {
			score := mz032ManualArrangementScore(candidate)
			if score.Reusable {
				checksum += uint64(score.NetBenefitNanos)
			}
		}
	}
	mz032BenchmarkSink = checksum
}

func BenchmarkMZ032SQLArrangementCostModel(b *testing.B) {
	candidates := mz032BenchmarkCandidates()
	model := NewSQLArrangementCostModel(SQLArrangementCostModelOptions{MemoryBudgetBytes: 1 << 20})
	b.ReportAllocs()
	b.ResetTimer()
	var checksum uint64
	for iteration := 0; iteration < b.N; iteration++ {
		for _, candidate := range candidates {
			score := model.Evaluate(candidate)
			if score.Reusable {
				checksum += uint64(score.NetBenefitNanos)
			}
		}
	}
	mz032BenchmarkSink = checksum
}
