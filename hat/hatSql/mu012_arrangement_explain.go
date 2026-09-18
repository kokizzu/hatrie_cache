package hatSql

import "strings"

const (
	maxSQLExplainArrangementEntries = 64
	maxSQLExplainArrangementText    = 256
	maxSQLExplainArrangementFields  = 16
)

// SQLArrangementMetadata describes one reusable physical arrangement that a
// source resolver can expose to EXPLAIN. It is planning metadata only; query
// execution never depends on this optional contract.
type SQLArrangementMetadata struct {
	Key            string   `json:"key"`
	Kind           string   `json:"kind"`
	Reused         bool     `json:"reused"`
	Fields         []string `json:"fields,omitempty"`
	Cardinality    int      `json:"cardinality,omitempty"`
	MemoryBytes    int      `json:"memory_bytes,omitempty"`
	Recommended    bool     `json:"recommended,omitempty"`
	MatchScore     int      `json:"match_score,omitempty"`
	Recommendation string   `json:"recommendation,omitempty"`
}

// SQLArrangementMetadataResolver optionally supplies bounded arrangement
// metadata for a source. A resolver error is treated as unavailable explain
// metadata so diagnostics cannot make an otherwise valid EXPLAIN fail.
type SQLArrangementMetadataResolver interface {
	ResolveSQLArrangementMetadata(name, key string) ([]SQLArrangementMetadata, error)
}

func resolveSQLArrangementMetadata(resolver SQLSourceResolver, source sqlSource) []SQLArrangementMetadata {
	if resolver == nil {
		return nil
	}
	metadataResolver, ok := resolver.(SQLArrangementMetadataResolver)
	if !ok {
		return nil
	}
	arrangements, err := metadataResolver.ResolveSQLArrangementMetadata(source.kind, source.key)
	if err != nil || len(arrangements) == 0 {
		return nil
	}
	if len(arrangements) > maxSQLExplainArrangementEntries {
		arrangements = arrangements[:maxSQLExplainArrangementEntries]
	}
	cloned := make([]SQLArrangementMetadata, 0, len(arrangements))
	for _, arrangement := range arrangements {
		arrangement.Key = cloneSQLExplainArrangementText(arrangement.Key)
		arrangement.Kind = cloneSQLExplainArrangementText(arrangement.Kind)
		arrangement.Fields = cloneSQLExplainArrangementFields(arrangement.Fields)
		cloned = append(cloned, arrangement)
	}
	return cloned
}

func cloneSQLArrangementMetadata(arrangements []SQLArrangementMetadata) []SQLArrangementMetadata {
	if len(arrangements) == 0 {
		return nil
	}
	cloned := make([]SQLArrangementMetadata, len(arrangements))
	copy(cloned, arrangements)
	for index := range cloned {
		cloned[index].Key = cloneSQLExplainArrangementText(cloned[index].Key)
		cloned[index].Kind = cloneSQLExplainArrangementText(cloned[index].Kind)
		cloned[index].Fields = cloneSQLExplainArrangementFields(cloned[index].Fields)
	}
	return cloned
}

func cloneSQLExplainArrangementFields(fields []string) []string {
	if len(fields) == 0 {
		return nil
	}
	if len(fields) > maxSQLExplainArrangementFields {
		fields = fields[:maxSQLExplainArrangementFields]
	}
	cloned := make([]string, len(fields))
	for index, field := range fields {
		cloned[index] = cloneSQLExplainArrangementText(field)
	}
	return cloned
}

func cloneSQLExplainArrangementText(value string) string {
	if len(value) > maxSQLExplainArrangementText {
		value = value[:maxSQLExplainArrangementText]
	}
	return strings.Clone(value)
}

func sqlExplainHasArrangementMetadata(steps []SQLExplainStep) bool {
	for _, step := range steps {
		if len(step.Arrangements) > 0 {
			return true
		}
	}
	return false
}
