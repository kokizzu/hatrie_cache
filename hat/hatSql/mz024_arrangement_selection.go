package hatSql

import "strings"

const (
	maxSQLArrangementSelectorFields     = 32
	maxSQLArrangementSelectorFieldBytes = 128
)

// SQLArrangementWorkload describes the query dimensions an arrangement may
// accelerate. Field order is retained for composite arrangement matching.
type SQLArrangementWorkload struct {
	FilterFields  []string
	GroupByFields []string
	OrderByFields []string
	JoinFields    []string
	orderedFields []string
}

// SQLArrangementRecommendation is a bounded, advisory arrangement choice.
// It never creates, drops, or changes an arrangement and is used by EXPLAIN.
type SQLArrangementRecommendation struct {
	Key    string
	Kind   string
	Score  int
	Reason string
}

type sqlArrangementMatch struct {
	index       int
	score       int
	matched     int
	filter      int
	group       int
	order       int
	join        int
	reused      bool
	memoryBytes int
	cardinality int
	key         string
	kind        string
}

// RecommendSQLArrangement selects the best existing arrangement for a query
// shape. An empty result means that no candidate matches a workload field.
// The selector is deterministic and bounded so EXPLAIN cannot retain an
// untrusted metadata payload indefinitely.
func RecommendSQLArrangement(candidates []SQLArrangementMetadata, workload SQLArrangementWorkload) SQLArrangementRecommendation {
	match, ok := sqlBestArrangementMatch(candidates, workload)
	if !ok {
		return SQLArrangementRecommendation{}
	}
	return SQLArrangementRecommendation{
		Key:    match.key,
		Kind:   match.kind,
		Score:  match.score,
		Reason: sqlArrangementRecommendationReason(match),
	}
}

func sqlMarkArrangementRecommendation(arrangements []SQLArrangementMetadata, workload SQLArrangementWorkload) {
	for index := range arrangements {
		arrangements[index].Recommended = false
		arrangements[index].MatchScore = 0
		arrangements[index].Recommendation = ""
	}
	match, ok := sqlBestArrangementMatch(arrangements, workload)
	if !ok {
		return
	}
	arrangements[match.index].Recommended = true
	arrangements[match.index].MatchScore = match.score
	arrangements[match.index].Recommendation = sqlArrangementRecommendationReason(match)
}

func sqlBestArrangementMatch(candidates []SQLArrangementMetadata, workload SQLArrangementWorkload) (sqlArrangementMatch, bool) {
	workload = sqlNormalizeArrangementWorkload(workload)
	if len(workload.FilterFields) == 0 && len(workload.GroupByFields) == 0 && len(workload.OrderByFields) == 0 && len(workload.JoinFields) == 0 {
		return sqlArrangementMatch{}, false
	}
	var best sqlArrangementMatch
	matched := false
	for index, candidate := range candidates {
		if strings.TrimSpace(candidate.Key) == "" {
			continue
		}
		current := sqlScoreArrangement(index, candidate, workload)
		if current.matched == 0 || current.score <= 0 {
			continue
		}
		if !matched || sqlArrangementMatchBetter(current, best) {
			best = current
			matched = true
		}
	}
	return best, matched
}

func sqlScoreArrangement(index int, candidate SQLArrangementMetadata, workload SQLArrangementWorkload) sqlArrangementMatch {
	match := sqlArrangementMatch{
		index:       index,
		reused:      candidate.Reused,
		memoryBytes: candidate.MemoryBytes,
		cardinality: candidate.Cardinality,
		key:         candidate.Key,
		kind:        candidate.Kind,
	}
	fields := candidate.Fields
	if len(fields) == 0 {
		fields = sqlArrangementFieldsFromKey(candidate.Key)
	}
	for fieldIndex, field := range fields {
		field = sqlArrangementNormalizeField(field)
		if field == "" {
			continue
		}
		weight, category := sqlArrangementWorkloadWeight(field, workload)
		if weight == 0 {
			continue
		}
		match.score += weight
		match.matched++
		switch category {
		case "filter":
			match.filter++
		case "group":
			match.group++
		case "order":
			match.order++
		case "join":
			match.join++
		}
		if sqlArrangementFieldAtWorkloadPosition(field, fieldIndex, workload) {
			match.score++
		}
	}
	kind := strings.ToLower(candidate.Kind)
	if match.group > 0 && (strings.Contains(kind, "aggregate") || strings.Contains(kind, "group")) {
		match.score += 2
	}
	if match.order > 0 && (strings.Contains(kind, "sorted") || strings.Contains(kind, "order")) {
		match.score += 2
	}
	if (match.filter > 0 || match.join > 0) && (strings.Contains(kind, "index") || strings.Contains(kind, "hash")) {
		match.score++
	}
	if candidate.Reused {
		match.score++
	}
	return match
}

func sqlArrangementMatchBetter(left, right sqlArrangementMatch) bool {
	if left.score != right.score {
		return left.score > right.score
	}
	if left.matched != right.matched {
		return left.matched > right.matched
	}
	if left.reused != right.reused {
		return left.reused
	}
	if left.memoryBytes != right.memoryBytes {
		if left.memoryBytes == 0 {
			return false
		}
		if right.memoryBytes == 0 {
			return true
		}
		return left.memoryBytes < right.memoryBytes
	}
	if left.cardinality != right.cardinality {
		if left.cardinality == 0 {
			return false
		}
		if right.cardinality == 0 {
			return true
		}
		return left.cardinality < right.cardinality
	}
	if left.key != right.key {
		return left.key < right.key
	}
	return left.kind < right.kind
}

func sqlArrangementWorkloadWeight(field string, workload SQLArrangementWorkload) (int, string) {
	if sqlArrangementContainsField(workload.FilterFields, field) {
		return 5, "filter"
	}
	if sqlArrangementContainsField(workload.GroupByFields, field) {
		return 7, "group"
	}
	if sqlArrangementContainsField(workload.OrderByFields, field) {
		return 4, "order"
	}
	if sqlArrangementContainsField(workload.JoinFields, field) {
		return 6, "join"
	}
	return 0, ""
}

func sqlArrangementFieldAtWorkloadPosition(field string, position int, workload SQLArrangementWorkload) bool {
	return position < len(workload.orderedFields) && field == workload.orderedFields[position]
}

func sqlArrangementContainsField(fields []string, wanted string) bool {
	for _, field := range fields {
		if sqlArrangementNormalizeField(field) == wanted {
			return true
		}
	}
	return false
}

func sqlNormalizeArrangementWorkload(workload SQLArrangementWorkload) SQLArrangementWorkload {
	workload.FilterFields = sqlNormalizeArrangementFields(workload.FilterFields)
	workload.GroupByFields = sqlNormalizeArrangementFields(workload.GroupByFields)
	workload.OrderByFields = sqlNormalizeArrangementFields(workload.OrderByFields)
	workload.JoinFields = sqlNormalizeArrangementFields(workload.JoinFields)
	workload.orderedFields = make([]string, 0, len(workload.FilterFields)+len(workload.GroupByFields)+len(workload.OrderByFields)+len(workload.JoinFields))
	workload.orderedFields = append(workload.orderedFields, workload.FilterFields...)
	workload.orderedFields = append(workload.orderedFields, workload.GroupByFields...)
	workload.orderedFields = append(workload.orderedFields, workload.OrderByFields...)
	workload.orderedFields = append(workload.orderedFields, workload.JoinFields...)
	return workload
}

func sqlNormalizeArrangementFields(fields []string) []string {
	if len(fields) > maxSQLArrangementSelectorFields {
		fields = fields[:maxSQLArrangementSelectorFields]
	}
	normalized := make([]string, 0, len(fields))
	for _, field := range fields {
		field = sqlArrangementNormalizeField(field)
		if field == "" || sqlArrangementContainsField(normalized, field) {
			continue
		}
		normalized = append(normalized, field)
	}
	return normalized
}

func sqlArrangementNormalizeField(field string) string {
	field = strings.ToLower(strings.TrimSpace(field))
	if separator := strings.LastIndexByte(field, '.'); separator >= 0 {
		field = field[separator+1:]
	}
	if len(field) > maxSQLArrangementSelectorFieldBytes {
		field = field[:maxSQLArrangementSelectorFieldBytes]
	}
	return field
}

func sqlArrangementFieldsFromKey(key string) []string {
	fields := strings.FieldsFunc(key, func(r rune) bool {
		switch r {
		case ',', ':', '/', '|', '=', '(', ')', '[', ']', '{', '}', ' ', '\t', '\n':
			return true
		default:
			return false
		}
	})
	return sqlNormalizeArrangementFields(fields)
}

func sqlArrangementRecommendationReason(match sqlArrangementMatch) string {
	categories := make([]string, 0, 4)
	if match.filter > 0 {
		categories = append(categories, "WHERE")
	}
	if match.group > 0 {
		categories = append(categories, "GROUP BY")
	}
	if match.order > 0 {
		categories = append(categories, "ORDER BY")
	}
	if match.join > 0 {
		categories = append(categories, "JOIN")
	}
	if len(categories) == 0 {
		return "matched query fields"
	}
	return "matched " + strings.Join(categories, ", ") + " fields"
}

func sqlArrangementWorkloadForQuery(query *sqlQuery) SQLArrangementWorkload {
	if query == nil {
		return SQLArrangementWorkload{}
	}
	shape := sqlProjectionAdvisorQueryShape(query)
	workload := SQLArrangementWorkload{
		FilterFields:  shape.filterFields,
		GroupByFields: shape.groupByFields,
		OrderByFields: shape.orderByFields,
	}
	for _, join := range query.joins {
		sqlProjectionAdvisorCollectFields(join.on, &workload.JoinFields)
	}
	return sqlNormalizeArrangementWorkload(workload)
}
