package hatSql

import (
	"errors"
	"math"
	"reflect"
)

var ErrQuerySubscriptionDeltaOverflow = errors.New("hatSql: query subscription delta multiplicity overflow")

// FoldQuerySubscriptionDeltas combines equal row updates by signed
// multiplicity. It preserves the first surviving row order, removes zero-sum
// rows, does not mutate input, and detaches the returned row maps.
func FoldQuerySubscriptionDeltas(deltas []QuerySubscriptionDelta) ([]QuerySubscriptionDelta, error) {
	return foldQuerySubscriptionDeltas(deltas, true)
}

func foldQuerySubscriptionDeltas(deltas []QuerySubscriptionDelta, cloneRows bool) ([]QuerySubscriptionDelta, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	if len(deltas) == 1 {
		if deltas[0].Diff == 0 {
			return nil, nil
		}
		if !cloneRows {
			return deltas, nil
		}
		return []QuerySubscriptionDelta{{
			Row:  cloneDifferentialRow(deltas[0].Row),
			Diff: deltas[0].Diff,
		}}, nil
	}
	if len(deltas) == 2 {
		if deltas[0].Diff == 0 {
			return foldQuerySubscriptionDeltas(deltas[1:], cloneRows)
		}
		if deltas[1].Diff == 0 {
			return foldQuerySubscriptionDeltas(deltas[:1], cloneRows)
		}
		if reflect.DeepEqual(deltas[0].Row, deltas[1].Row) {
			return combineQuerySubscriptionDeltas(deltas[0], deltas[1], cloneRows)
		}
		// NaN is the one common value for which DeepEqual is false while the
		// canonical SQL key is equal; retain the exact-key fallback for it.
		if !querySubscriptionRowsContainNaN(deltas[0].Row, deltas[1].Row) {
			if !cloneRows {
				return deltas, nil
			}
			return []QuerySubscriptionDelta{
				{Row: cloneDifferentialRow(deltas[0].Row), Diff: deltas[0].Diff},
				{Row: cloneDifferentialRow(deltas[1].Row), Diff: deltas[1].Diff},
			}, nil
		}
		leftKey := querySubscriptionRowKey(deltas[0].Row)
		rightKey := querySubscriptionRowKey(deltas[1].Row)
		if leftKey != rightKey {
			if !cloneRows {
				return deltas, nil
			}
			return []QuerySubscriptionDelta{
				{Row: cloneDifferentialRow(deltas[0].Row), Diff: deltas[0].Diff},
				{Row: cloneDifferentialRow(deltas[1].Row), Diff: deltas[1].Diff},
			}, nil
		}
		return combineQuerySubscriptionDeltas(deltas[0], deltas[1], cloneRows)
	}
	indexes := make(map[string]int, len(deltas))
	folded := make([]QuerySubscriptionDelta, 0, len(deltas))
	for _, delta := range deltas {
		if delta.Diff == 0 {
			continue
		}
		key := querySubscriptionRowKey(delta.Row)
		index, exists := indexes[key]
		if !exists {
			if cloneRows {
				delta.Row = cloneDifferentialRow(delta.Row)
			}
			indexes[key] = len(folded)
			folded = append(folded, delta)
			continue
		}
		combined, ok := addDifferentialCounts(folded[index].Diff, delta.Diff)
		if !ok {
			return nil, ErrQuerySubscriptionDeltaOverflow
		}
		folded[index].Diff = combined
		if combined == 0 {
			delete(indexes, key)
		}
	}
	result := folded[:0]
	for _, delta := range folded {
		if delta.Diff != 0 {
			result = append(result, delta)
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func combineQuerySubscriptionDeltas(left, right QuerySubscriptionDelta, cloneRows bool) ([]QuerySubscriptionDelta, error) {
	combined, ok := addDifferentialCounts(left.Diff, right.Diff)
	if !ok {
		return nil, ErrQuerySubscriptionDeltaOverflow
	}
	if combined == 0 {
		return nil, nil
	}
	if cloneRows {
		left.Row = cloneDifferentialRow(left.Row)
	}
	left.Diff = combined
	return []QuerySubscriptionDelta{left}, nil
}

func querySubscriptionRowsContainNaN(rows ...Row) bool {
	for _, row := range rows {
		for _, value := range row {
			if querySubscriptionValueContainsNaN(value) {
				return true
			}
		}
	}
	return false
}

func querySubscriptionValueContainsNaN(value interface{}) bool {
	switch value := value.(type) {
	case float32:
		return math.IsNaN(float64(value))
	case float64:
		return math.IsNaN(value)
	case complex64:
		return math.IsNaN(float64(real(value))) || math.IsNaN(float64(imag(value)))
	case complex128:
		return math.IsNaN(real(value)) || math.IsNaN(imag(value))
	case Row:
		return querySubscriptionRowsContainNaN(value)
	case map[string]interface{}:
		return querySubscriptionRowsContainNaN(Row(value))
	case []interface{}:
		for _, item := range value {
			if querySubscriptionValueContainsNaN(item) {
				return true
			}
		}
	}
	return false
}

// FoldQuerySubscriptionDeltaBatch copies batch metadata and folds its signed
// row updates. The input batch and its Columns slice are not mutated.
func FoldQuerySubscriptionDeltaBatch(batch QuerySubscriptionDeltaBatch) (QuerySubscriptionDeltaBatch, error) {
	folded, err := foldQuerySubscriptionDeltaBatch(batch, true)
	if err != nil {
		return QuerySubscriptionDeltaBatch{}, err
	}
	return folded, nil
}

func foldQuerySubscriptionDeltaBatch(batch QuerySubscriptionDeltaBatch, cloneRows bool) (QuerySubscriptionDeltaBatch, error) {
	folded, err := foldQuerySubscriptionDeltas(batch.Deltas, cloneRows)
	if err != nil {
		return QuerySubscriptionDeltaBatch{}, err
	}
	batch.Deltas = folded
	if cloneRows {
		batch.Columns = append([]string(nil), batch.Columns...)
	}
	return batch, nil
}
