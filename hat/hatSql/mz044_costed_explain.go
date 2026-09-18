package hatSql

import "strings"

const (
	defaultSQLExplainCPUCostPerRow   = 1
	defaultSQLExplainMemoryPerRow    = 64
	defaultSQLExplainScanCPUWeight   = 1
	defaultSQLExplainFilterCPUWeight = 2
	defaultSQLExplainJoinCPUWeight   = 4
	defaultSQLExplainAggregateWeight = 3
	defaultSQLExplainSortCPUWeight   = 4
	defaultSQLExplainStateMemoryUnit = 2
)

// SQLExplainCostOptions controls the units used by CostSQLExplainSteps. The
// estimates are deliberately heuristic and comparable within one plan, not a
// claim about allocator bytes or wall-clock nanoseconds.
type SQLExplainCostOptions struct {
	CPUCostPerRow     int
	MemoryBytesPerRow int
}

// CostSQLExplainSteps returns an independent plan copy with bounded heuristic
// CPU and memory estimates for steps that already have an estimated row count.
// It never executes a source or changes query execution.
func CostSQLExplainSteps(steps []ExplainStep, options SQLExplainCostOptions) []ExplainStep {
	if len(steps) == 0 {
		return nil
	}
	if options.CPUCostPerRow <= 0 {
		options.CPUCostPerRow = defaultSQLExplainCPUCostPerRow
	}
	if options.MemoryBytesPerRow <= 0 {
		options.MemoryBytesPerRow = defaultSQLExplainMemoryPerRow
	}
	costed := make([]ExplainStep, len(steps))
	for index, source := range steps {
		step := cloneExplainDataflowStep(source)
		if step.EstimatedRows != nil && *step.EstimatedRows >= 0 {
			cpuWeight, memoryWeight := sqlExplainCostWeights(step.Node)
			cost := sqlExplainCostProduct(*step.EstimatedRows, options.CPUCostPerRow, cpuWeight)
			memory := sqlExplainCostProduct(*step.EstimatedRows, options.MemoryBytesPerRow, memoryWeight)
			step.EstimatedCost = &cost
			step.EstimatedMemoryBytes = &memory
		}
		costed[index] = step
	}
	return costed
}

func sqlExplainHasCost(steps []ExplainStep) bool {
	for _, step := range steps {
		if step.EstimatedCost != nil || step.EstimatedMemoryBytes != nil {
			return true
		}
	}
	return false
}

func sqlExplainCostWeights(node string) (int, int) {
	name := strings.ToUpper(strings.TrimSpace(node))
	switch {
	case strings.Contains(name, "JOIN"):
		return defaultSQLExplainJoinCPUWeight, defaultSQLExplainStateMemoryUnit
	case name == "AGGREGATE" || name == "GROUP AGGREGATE":
		return defaultSQLExplainAggregateWeight, defaultSQLExplainStateMemoryUnit
	case name == "SORT" || name == "TOP-N" || name == "LIMIT BY":
		return defaultSQLExplainSortCPUWeight, defaultSQLExplainStateMemoryUnit
	case name == "FILTER" || name == "PREWHERE" || name == "HAVING":
		return defaultSQLExplainFilterCPUWeight, defaultSQLExplainCPUCostPerRow
	case name == "SCAN" || name == "VALUES":
		return defaultSQLExplainScanCPUWeight, defaultSQLExplainCPUCostPerRow
	default:
		return defaultSQLExplainScanCPUWeight, defaultSQLExplainCPUCostPerRow
	}
}

func sqlExplainCostProduct(rows, unit, weight int) int {
	if rows <= 0 || unit <= 0 || weight <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if rows > maxInt/unit || rows*unit > maxInt/weight {
		return maxInt
	}
	return rows * unit * weight
}
