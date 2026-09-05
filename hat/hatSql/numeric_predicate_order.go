package hatSql

// sqlColumnarOrderNumericPredicates puts the most selective known mark
// predicate first. Direct numeric comparisons are pure, so this preserves
// their SQL result while avoiding later field reads for rejected rows.
func sqlColumnarOrderNumericPredicates(segments *ColumnarNumericSegments, predicates []sqlColumnarNumericFilter) []sqlColumnarNumericFilter {
	if segments == nil || len(predicates) < 2 || len(predicates) > 8 {
		return predicates
	}
	var scores [8]int
	var known [8]bool
	for index, predicate := range predicates {
		fieldSegments, available := segments.Columns[predicate.field]
		if !available || len(fieldSegments) == 0 {
			continue
		}
		known[index] = true
		for _, segment := range fieldSegments {
			if segment.Valid && sqlColumnarNumericSegmentMatches(segment, predicate) {
				scores[index]++
			}
		}
	}
	for index := 1; index < len(predicates); index++ {
		if !known[index] {
			continue
		}
		position := index
		for position > 0 && (!known[position-1] || scores[index] < scores[position-1]) {
			predicates[position], predicates[position-1] = predicates[position-1], predicates[position]
			known[position], known[position-1] = known[position-1], known[position]
			scores[position], scores[position-1] = scores[position-1], scores[position]
			position--
			index = position
		}
	}
	return predicates
}
