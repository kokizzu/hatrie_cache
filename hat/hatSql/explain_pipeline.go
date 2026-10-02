package hatSql

import (
	"fmt"
	"strings"
)

func explainSQLPipelineQuery(query *sqlQuery, resolver SQLSourceResolver) (SQLQueryResult, error) {
	if query == nil {
		return SQLQueryResult{}, fmt.Errorf("SQL pipeline query is nil")
	}
	if query.analyze {
		return SQLQueryResult{}, fmt.Errorf("EXPLAIN PIPELINE ANALYZE is not supported")
	}
	steps, partitioning := sqlExplainPipelineStepsWithResolverAndPartitioning(query, resolver)
	if query.explainCost {
		steps = CostSQLExplainSteps(steps, SQLExplainCostOptions{})
	}
	hasArrangementMetadata := sqlExplainHasArrangementMetadata(steps)
	hasPartitioningMetadata := len(partitioning) > 0
	hasExplainCost := sqlExplainHasCost(steps)
	columns := []string{"node", "detail", "stage", "worker", "workers", "estimated_rows"}
	if hasExplainCost {
		columns = append(columns, "estimated_cost", "estimated_memory_bytes")
	}
	if hasArrangementMetadata {
		columns = append(columns, "arrangements")
	}
	if hasPartitioningMetadata {
		columns = append(columns, "partitioning")
	}
	result := SQLQueryResult{
		Columns:      columns,
		Rows:         make([]SQLRow, 0, len(steps)),
		Plan:         steps,
		Partitioning: partitioning,
	}
	for index, step := range steps {
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
		if step.EstimatedCost != nil {
			row["estimated_cost"] = *step.EstimatedCost
		}
		if step.EstimatedMemoryBytes != nil {
			row["estimated_memory_bytes"] = *step.EstimatedMemoryBytes
		}
		if hasArrangementMetadata && len(step.Arrangements) > 0 {
			row["arrangements"] = cloneSQLArrangementMetadata(step.Arrangements)
		}
		if declaration := sqlExplainPartitioningForStep(partitioning, index); declaration != nil {
			row["partitioning"] = declaration
		}
		result.Rows = append(result.Rows, row)
	}
	return result, nil
}

func sqlExplainPipelineSteps(query *sqlQuery) []SQLExplainStep {
	return sqlExplainPipelineStepsWithResolver(query, nil)
}

func sqlExplainPipelineStepsWithResolver(query *sqlQuery, resolver SQLSourceResolver) []SQLExplainStep {
	steps := sqlExplainStepsWithResolver(query, resolver)
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

func sqlExplainPipelineStepsWithResolverAndPartitioning(query *sqlQuery, resolver SQLSourceResolver) ([]SQLExplainStep, []ExplainPartitioningAnnotation) {
	steps, partitioning := sqlExplainStepsWithResolverAndPartitioning(query, resolver)
	stage := 1
	for index := range steps {
		if index > 0 && sqlExplainPipelineBoundary(steps[index].Node) {
			stage++
		}
		steps[index].Stage = stage
		steps[index].Worker = 1
		steps[index].Workers = 1
	}
	return steps, partitioning
}

func sqlExplainPipelineBoundary(node string) bool {
	name := strings.ToUpper(strings.TrimSpace(node))
	return name == "AGGREGATE" || name == "DISTINCT" || name == "SORT" || name == "LIMIT BY" || name == "SET" || strings.HasSuffix(name, " JOIN")
}
