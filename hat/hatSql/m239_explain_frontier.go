package hatSql

import (
	"strconv"
	"strings"
)

const (
	sqlExplainLogicalTimestampNotice    = "LOGICAL_TIMESTAMP"
	sqlExplainFrontierRequirementNotice = "FRONTIER_REQUIREMENT"
)

// annotateSQLExplainTemporalRequirements makes the temporal contract visible
// in the existing machine-readable plan without changing query execution.
// The default path returns before touching the plan, so ordinary queries keep
// their existing allocations and explain shape.
func annotateSQLExplainTemporalRequirements(steps []ExplainStep, options SQLQueryOptions) {
	if len(steps) == 0 || (options.AsOfFrontier == nil && !options.RequireSourceFrontier) {
		return
	}
	stepIndex := 0
	for index := range steps {
		if strings.HasSuffix(strings.TrimSpace(steps[index].Node), "SCAN") {
			stepIndex = index
			break
		}
	}
	step := &steps[stepIndex]
	notices := make([]ExplainNotice, 0, 2)
	if options.AsOfFrontier != nil {
		notices = append(notices, ExplainNotice{
			Code:   sqlExplainLogicalTimestampNotice,
			Detail: "as_of_frontier=" + strconv.FormatUint(*options.AsOfFrontier, 10),
		})
	}
	if options.RequireSourceFrontier {
		notices = append(notices, ExplainNotice{
			Code:   sqlExplainFrontierRequirementNotice,
			Detail: "required_source_frontier=" + strconv.FormatUint(options.RequiredSourceFrontier, 10),
		})
	}
	if len(step.Notices) == 0 {
		step.Notices = notices
		return
	}
	step.Notices = append(step.Notices, notices...)
}
