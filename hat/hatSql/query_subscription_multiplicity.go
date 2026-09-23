package hatSql

import (
	"errors"
	"fmt"
)

// ErrQuerySubscriptionDeltaOverflow reports an int64 multiplicity overflow
// while consolidating equal query-subscription rows.
var ErrQuerySubscriptionDeltaOverflow = errors.New("hatSql: query subscription delta multiplicity overflow")

// ConsolidateQuerySubscriptionDeltas combines entries with the same complete
// row image, preserving their signed multiplicity. Zero-sum entries are
// removed, first-surviving order is retained, and input rows are cloned so
// callers can safely mutate the result. The input slice and maps are not
// modified.
func ConsolidateQuerySubscriptionDeltas(deltas []QuerySubscriptionDelta) ([]QuerySubscriptionDelta, error) {
	return consolidateQuerySubscriptionDeltas(deltas, true)
}

func consolidateQuerySubscriptionDeltas(deltas []QuerySubscriptionDelta, cloneRows bool) ([]QuerySubscriptionDelta, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	indexes := make(map[string]int, len(deltas))
	consolidated := make([]QuerySubscriptionDelta, 0, len(deltas))
	for _, delta := range deltas {
		if delta.Diff == 0 {
			continue
		}
		key := querySubscriptionRowKey(delta.Row)
		outputIndex, exists := indexes[key]
		if !exists {
			indexes[key] = len(consolidated)
			consolidated = append(consolidated, delta)
			continue
		}
		combined, ok := addDifferentialCounts(consolidated[outputIndex].Diff, delta.Diff)
		if !ok {
			return nil, fmt.Errorf("row %q: %w", key, ErrQuerySubscriptionDeltaOverflow)
		}
		consolidated[outputIndex].Diff = combined
		if combined == 0 {
			delete(indexes, key)
		}
	}
	result := consolidated[:0]
	for _, delta := range consolidated {
		if delta.Diff != 0 {
			result = append(result, delta)
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	if cloneRows {
		for index := range result {
			result[index].Row = cloneDifferentialRow(result[index].Row)
		}
	}
	return result, nil
}

func querySubscriptionDeltasChanged(before, after []QuerySubscriptionDelta) bool {
	if len(before) != len(after) {
		return true
	}
	for index := range before {
		if before[index].Diff != after[index].Diff || querySubscriptionRowKey(before[index].Row) != querySubscriptionRowKey(after[index].Row) {
			return true
		}
	}
	return false
}
