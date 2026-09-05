package hatSql

import (
	"fmt"
	"strings"
)

func explainSQLPipelineQuery(query *sqlQuery) (SQLQueryResult, error) {
	if query == nil {
		return SQLQueryResult{}, fmt.Errorf("SQL pipeline query is nil")
	}
	if query.analyze {
		return SQLQueryResult{}, fmt.Errorf("EXPLAIN PIPELINE ANALYZE is not supported")
	}
	steps := sqlExplainPipelineSteps(query)
	result := SQLQueryResult{
		Columns: []string{"node", "detail", "stage", "worker", "workers", "estimated_rows"},
		Rows:    make([]SQLRow, 0, len(steps)),
		Plan:    steps,
	}
	for _, step := range steps {
		row := SQLRow{
			"node":    step.Node,
			"detail":  step.Detail,
			"stage":   step.Stage,
			"worker":  step.Worker,
			"workers": step.Workers,
		}
		if step.EstimatedRows != nil {
			row["estimated_rows"] = *step.EstimatedRows
		}
		result.Rows = append(result.Rows, row)
	}
	return result, nil
}

func sqlExplainPipelineSteps(query *sqlQuery) []SQLExplainStep {
	steps := sqlExplainSteps(query)
	stage := 1
	for index := range steps {
		if index > 0 && sqlExplainPipelineBoundary(steps[index].Node) {
			stage++
		}
		steps[index].Stage = stage
		steps[index].Worker = 1
		steps[index].Workers = 1
	}
	return steps
}

func sqlExplainPipelineBoundary(node string) bool {
	name := strings.ToUpper(strings.TrimSpace(node))
	return name == "AGGREGATE" || name == "DISTINCT" || name == "SORT" || name == "LIMIT BY" || name == "SET" || strings.HasSuffix(name, " JOIN")
}
