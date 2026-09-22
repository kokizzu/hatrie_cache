package hatSql

const (
	// SQLPlanSnapshotFormat identifies the stable wire shape of a temporal
	// explain snapshot.
	SQLPlanSnapshotFormat = "hatrie-cache-sql-plan-snapshot/v1"
)

// SQLPlanSnapshotOptions enables an immutable plan snapshot on one materialized
// query result. The pointer itself is the opt-in switch so future snapshot
// controls can be added without changing SQLQueryOptions semantics.
type SQLPlanSnapshotOptions struct{}

// SQLPlanSnapshot captures the plan together with the temporal contract that
// produced it. StartedAtUnixNano is a wall-clock diagnostic timestamp, while
// frontier values are logical source positions and are never inferred from
// wall-clock time.
type SQLPlanSnapshot struct {
	Format                 string        `json:"format"`
	QueryID                string        `json:"query_id,omitempty"`
	StartedAtUnixNano      int64         `json:"started_at_unix_nano"`
	RequiredSourceFrontier *uint64       `json:"required_source_frontier,omitempty"`
	AsOfFrontier           *uint64       `json:"as_of_frontier,omitempty"`
	Steps                  []ExplainStep `json:"steps"`
}

func cloneSQLPlanSnapshotFrontier(frontier *uint64) *uint64 {
	if frontier == nil {
		return nil
	}
	value := *frontier
	return &value
}

func cloneSQLPlanSnapshot(snapshot *SQLPlanSnapshot) *SQLPlanSnapshot {
	if snapshot == nil {
		return nil
	}
	clone := *snapshot
	clone.RequiredSourceFrontier = cloneSQLPlanSnapshotFrontier(snapshot.RequiredSourceFrontier)
	clone.AsOfFrontier = cloneSQLPlanSnapshotFrontier(snapshot.AsOfFrontier)
	clone.Steps = cloneSQLPlanSnapshotSteps(snapshot.Steps)
	return &clone
}

func cloneSQLPlanSnapshotSteps(steps []ExplainStep) []ExplainStep {
	cloned := cloneMaterializedExplainSteps(steps)
	for index, step := range steps {
		cloned[index].Alternatives = append([]ExplainAlternative(nil), step.Alternatives...)
		cloned[index].Notices = append([]ExplainNotice(nil), step.Notices...)
		if step.Pruning != nil {
			cloned[index].Pruning = cloneExplainPruning(step.Pruning)
		}
		cloned[index].Lineage = make([]ColumnLineage, len(step.Lineage))
		for lineageIndex, lineage := range step.Lineage {
			cloned[index].Lineage[lineageIndex] = ColumnLineage{
				Output:       lineage.Output,
				SourceFields: append([]string(nil), lineage.SourceFields...),
			}
		}
	}
	return cloned
}
