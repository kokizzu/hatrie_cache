package hatSql

import "errors"

// ErrQuerySubscriptionDeltaOverflow reports a signed multiplicity sum that
// cannot be represented by int64.
var ErrQuerySubscriptionDeltaOverflow = errors.New("hatSql: query subscription delta multiplicity overflowed")

// ConsolidateQuerySubscriptionDeltas combines equal row identities while
// preserving signed multiplicity and the first surviving row order. The input
// slice and row maps are not mutated; output rows own their map and []byte
// values. Zero-sum identities are removed.
func ConsolidateQuerySubscriptionDeltas(deltas []QuerySubscriptionDelta) ([]QuerySubscriptionDelta, error) {
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
			delta.Row = cloneDifferentialRow(delta.Row)
			indexes[key] = len(consolidated)
			consolidated = append(consolidated, delta)
			continue
		}

		combined, ok := addDifferentialCounts(consolidated[outputIndex].Diff, delta.Diff)
		if !ok {
			return nil, ErrQuerySubscriptionDeltaOverflow
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
	return result, nil
}

// ConsolidateQuerySubscriptionDeltaBatch returns a batch with equal row
// updates folded before forwarding. Batch metadata and columns are copied;
// overflow returns an empty batch and leaves the input untouched.
func ConsolidateQuerySubscriptionDeltaBatch(batch QuerySubscriptionDeltaBatch) (QuerySubscriptionDeltaBatch, error) {
	deltas, err := ConsolidateQuerySubscriptionDeltas(batch.Deltas)
	if err != nil {
		return QuerySubscriptionDeltaBatch{}, err
	}
	batch.Columns = append([]string(nil), batch.Columns...)
	batch.Deltas = deltas
	return batch, nil
}
