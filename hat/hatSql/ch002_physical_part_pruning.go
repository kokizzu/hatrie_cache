package hatSql

import "math"

func pruneSQLColumnarSourceParts(parts []ColumnarSourcePart, predicates []sqlColumnarNumericFilter) []ColumnarSourcePart {
	if len(parts) < 2 || len(predicates) == 0 {
		return parts
	}
	selected := make([]ColumnarSourcePart, 0, len(parts))
	pruned := false
	for _, part := range parts {
		if sqlColumnarSourcePartMayMatch(part, predicates) {
			selected = append(selected, part)
			continue
		}
		pruned = true
	}
	if !pruned {
		return parts
	}
	return selected
}

func sqlColumnarSourcePartSelection(parts []ColumnarSourcePart, predicates []sqlColumnarNumericFilter) (index, count int) {
	if len(parts) == 0 {
		return -1, 0
	}
	if len(predicates) == 0 {
		return -1, len(parts)
	}
	index = -1
	for partIndex, part := range parts {
		if !sqlColumnarSourcePartMayMatch(part, predicates) {
			continue
		}
		index = partIndex
		count++
		if count > 1 {
			return -1, count
		}
	}
	return index, count
}

func sqlColumnarSourcePartMayMatch(part ColumnarSourcePart, predicates []sqlColumnarNumericFilter) bool {
	segments := part.Segments
	if segments == nil || segments.RowsPerSegment <= 0 || part.Batch.Rows <= 0 {
		return true
	}
	segmentCount := (part.Batch.Rows + segments.RowsPerSegment - 1) / segments.RowsPerSegment
	if !sqlColumnarSourcePartSparsePrimaryMetadataValid(segments, segmentCount) {
		return true
	}
	start, end, used := sqlColumnarSparsePrimarySegmentRangeWithComposite(segments, predicates, segmentCount)
	if !used || start < 0 || end < start || end > segmentCount {
		return true
	}
	return start < end
}

func sqlColumnarSourcePartSparsePrimaryMetadataValid(segments *ColumnarNumericSegments, segmentCount int) bool {
	if segments == nil || segmentCount <= 0 {
		return false
	}
	if segments.SparsePrimaryField != "" {
		bounds, ok := segments.Columns[segments.SparsePrimaryField]
		return ok && len(bounds) == segmentCount && sqlColumnarSourceBoundsOrdered(bounds)
	}
	fields := segments.SparsePrimaryFields
	if len(fields) < 2 {
		return true
	}
	fieldCount := len(fields)
	if fieldCount > typedTableSparsePrimaryMaxFields || len(segments.SparsePrimaryTupleMinimum) != segmentCount*fieldCount || len(segments.SparsePrimaryTupleMaximum) != segmentCount*fieldCount {
		return false
	}
	for segment := 0; segment < segmentCount; segment++ {
		minimum := segments.SparsePrimaryTupleMinimum[segment*fieldCount : (segment+1)*fieldCount]
		maximum := segments.SparsePrimaryTupleMaximum[segment*fieldCount : (segment+1)*fieldCount]
		for index := range minimum {
			if math.IsNaN(minimum[index]) || math.IsNaN(maximum[index]) {
				return false
			}
		}
		if sqlColumnarCompareNumericTuple(minimum, maximum) > 0 {
			return false
		}
		if segment > 0 {
			previous := segments.SparsePrimaryTupleMaximum[(segment-1)*fieldCount : segment*fieldCount]
			if sqlColumnarCompareNumericTuple(previous, minimum) > 0 {
				return false
			}
		}
	}
	return true
}

func sqlColumnarSourceBoundsOrdered(bounds []ColumnarNumericSegment) bool {
	var previous ColumnarNumericSegment
	for index, current := range bounds {
		if !current.Valid || math.IsNaN(current.Minimum) || math.IsNaN(current.Maximum) || current.Minimum > current.Maximum {
			return false
		}
		if index > 0 && previous.Maximum > current.Minimum {
			return false
		}
		previous = current
	}
	return len(bounds) > 0
}

func resolveSQLColumnarQuerySource(query *sqlQuery, resolver ColumnarSourceResolver, fields []string) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	if partsResolver, ok := resolver.(ColumnarPartsSourceResolver); ok {
		parts, available, err := partsResolver.BorrowSQLColumnarSourceParts(query.from.kind, query.from.key, fields)
		if err != nil {
			return ColumnarBatch{}, nil, false, err
		}
		if available {
			index, count := sqlColumnarSourcePartSelection(parts, sqlColumnarQueryNumericPredicates(query))
			switch count {
			case 0:
				return ColumnarBatch{Columns: make(map[string][]interface{})}, nil, true, nil
			case 1:
				return parts[index].Batch, parts[index].Segments, true, nil
			}
		}
	}
	return resolveSQLColumnarSource(resolver, query.from.kind, query.from.key, fields)
}

func sqlColumnarQueryNumericPredicates(query *sqlQuery) []sqlColumnarNumericFilter {
	if query == nil || query.from == nil {
		return nil
	}
	predicates, supported := sqlColumnarNumericConjunction(sqlCombinedWhere(query), query.from.alias)
	if !supported {
		return nil
	}
	return predicates
}
