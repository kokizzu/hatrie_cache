package hatSql

import "sort"

// SQLArrangementCostCandidate describes the expected cost and reuse of a
// caller-managed SQL arrangement such as an index, projection, or ordered
// layout. Costs are arbitrary comparable units; nanoseconds are recommended.
type SQLArrangementCostCandidate struct {
	Key              string
	Field            string
	Kind             string
	BuildCostNanos   uint64
	ProbeCostNanos   uint64
	ScanCostNanos    uint64
	MaintenanceNanos uint64
	ExpectedReads    uint64
	ExpectedWrites   uint64
	MemoryBytes      uint64
}

// SQLArrangementCostModelOptions controls the reuse decision. A zero memory
// budget means unlimited memory. A zero minimum benefit uses a positive
// one-unit threshold, so break-even arrangements are not selected.
type SQLArrangementCostModelOptions struct {
	MemoryBudgetBytes      uint64
	MinimumNetBenefitNanos uint64
}

// SQLArrangementCostScore is the deterministic cost and reuse score for one
// arrangement candidate. Reusable is advisory only; the model never creates,
// drops, or changes a stored arrangement.
type SQLArrangementCostScore struct {
	Candidate            SQLArrangementCostCandidate
	ReadSavingsNanos     uint64
	TotalCostNanos       uint64
	ExpectedBenefitNanos uint64
	NetBenefitNanos      int64
	PaybackReads         uint64
	FitsMemory           bool
	Reusable             bool
}

// SQLArrangementCostModel evaluates caller-supplied arrangement estimates.
// It is opt-in and has no effect on SQL execution unless a caller uses the
// returned score to choose a layout.
type SQLArrangementCostModel struct {
	options SQLArrangementCostModelOptions
}

// NewSQLArrangementCostModel creates an opt-in arrangement reuse model.
func NewSQLArrangementCostModel(options SQLArrangementCostModelOptions) *SQLArrangementCostModel {
	if options.MinimumNetBenefitNanos == 0 {
		options.MinimumNetBenefitNanos = 1
	}
	return &SQLArrangementCostModel{options: options}
}

// Evaluate scores one candidate without allocating. A payback of zero means
// the candidate has no up-front cost; positive read savings is still required.
func (model *SQLArrangementCostModel) Evaluate(candidate SQLArrangementCostCandidate) SQLArrangementCostScore {
	options := SQLArrangementCostModelOptions{MinimumNetBenefitNanos: 1}
	if model != nil {
		options = model.options
		if options.MinimumNetBenefitNanos == 0 {
			options.MinimumNetBenefitNanos = 1
		}
	}

	readSavings := uint64(0)
	if candidate.ScanCostNanos > candidate.ProbeCostNanos {
		readSavings = candidate.ScanCostNanos - candidate.ProbeCostNanos
	}
	totalCost := sqlArrangementSaturatingAdd(
		candidate.BuildCostNanos,
		sqlArrangementSaturatingMul(candidate.MaintenanceNanos, candidate.ExpectedWrites),
	)
	expectedBenefit := sqlArrangementSaturatingMul(readSavings, candidate.ExpectedReads)
	netBenefit := sqlArrangementNetBenefit(expectedBenefit, totalCost)
	paybackReads := uint64(0)
	if readSavings > 0 && totalCost > 0 {
		paybackReads = (totalCost-1)/readSavings + 1
	}
	fitsMemory := options.MemoryBudgetBytes == 0 || candidate.MemoryBytes <= options.MemoryBudgetBytes
	reusable := fitsMemory && readSavings > 0 && paybackReads <= candidate.ExpectedReads && sqlArrangementMeetsMinimum(netBenefit, options.MinimumNetBenefitNanos)

	return SQLArrangementCostScore{
		Candidate:            candidate,
		ReadSavingsNanos:     readSavings,
		TotalCostNanos:       totalCost,
		ExpectedBenefitNanos: expectedBenefit,
		NetBenefitNanos:      netBenefit,
		PaybackReads:         paybackReads,
		FitsMemory:           fitsMemory,
		Reusable:             reusable,
	}
}

// Rank returns independent scores ordered by reusable status, descending net
// benefit, ascending payback, ascending memory, and stable candidate identity.
func (model *SQLArrangementCostModel) Rank(candidates []SQLArrangementCostCandidate) []SQLArrangementCostScore {
	return model.RankInto(nil, candidates)
}

// RankInto fills and returns dst, reusing its backing array when it has enough
// capacity. The input candidates are never modified.
func (model *SQLArrangementCostModel) RankInto(dst []SQLArrangementCostScore, candidates []SQLArrangementCostCandidate) []SQLArrangementCostScore {
	if cap(dst) < len(candidates) {
		dst = make([]SQLArrangementCostScore, len(candidates))
	} else {
		dst = dst[:len(candidates)]
	}
	for index, candidate := range candidates {
		dst[index] = model.Evaluate(candidate)
	}
	sort.SliceStable(dst, func(left, right int) bool {
		first, second := dst[left], dst[right]
		if first.Reusable != second.Reusable {
			return first.Reusable
		}
		if first.NetBenefitNanos != second.NetBenefitNanos {
			return first.NetBenefitNanos > second.NetBenefitNanos
		}
		if first.PaybackReads != second.PaybackReads {
			return first.PaybackReads < second.PaybackReads
		}
		if first.Candidate.MemoryBytes != second.Candidate.MemoryBytes {
			return first.Candidate.MemoryBytes < second.Candidate.MemoryBytes
		}
		if first.Candidate.Key != second.Candidate.Key {
			return first.Candidate.Key < second.Candidate.Key
		}
		if first.Candidate.Field != second.Candidate.Field {
			return first.Candidate.Field < second.Candidate.Field
		}
		return first.Candidate.Kind < second.Candidate.Kind
	})
	return dst
}

func sqlArrangementSaturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

func sqlArrangementSaturatingMul(left, right uint64) uint64 {
	if left == 0 || right == 0 {
		return 0
	}
	if left > ^uint64(0)/right {
		return ^uint64(0)
	}
	return left * right
}

func sqlArrangementNetBenefit(benefit, cost uint64) int64 {
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

func sqlArrangementMeetsMinimum(netBenefit int64, minimum uint64) bool {
	if netBenefit <= 0 || minimum > uint64(1<<63-1) {
		return false
	}
	return uint64(netBenefit) >= minimum
}
