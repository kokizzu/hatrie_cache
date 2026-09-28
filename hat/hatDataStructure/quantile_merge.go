package hatDataStructure

import "errors"

var ErrQuantileSketchMergeIncompatible = errors.New("hatriecache: quantile sketches have incompatible epsilon")

// Merge combines two compatible bounded-rank summaries without replaying the
// source values. The merged summary keeps the receiver epsilon and remains
// bounded by the combined rank allowance.
func (sketch *QuantileSketch) Merge(other QuantileSketch) error {
	if sketch == nil || sketch.epsilon == 0 || other.epsilon == 0 || sketch.epsilon != other.epsilon {
		return ErrQuantileSketchMergeIncompatible
	}
	if other.count == 0 {
		return nil
	}
	if sketch.count == 0 {
		copySummary := make([]QuantileSketchSample, len(other.summary))
		copy(copySummary, other.summary)
		candidate := QuantileSketch{epsilon: sketch.epsilon, count: other.count, summary: copySummary}
		if err := ValidateQuantileSketchSnapshot(candidate.Snapshot()); err != nil {
			return err
		}
		*sketch = candidate
		return nil
	}
	if err := ValidateQuantileSketchSnapshot(sketch.Snapshot()); err != nil {
		return err
	}
	if err := ValidateQuantileSketchSnapshot(other.Snapshot()); err != nil {
		return err
	}
	candidate := QuantileSketch{
		epsilon: sketch.epsilon,
		count:   saturatingAddUint64Quantile(sketch.count, other.count),
		summary: mergeQuantileSketchSummaries(sketch.summary, other.summary),
	}
	if len(candidate.summary) > 1 {
		candidate.summary[0].Delta = 0
		candidate.summary[len(candidate.summary)-1].Delta = 0
	}
	candidate.compress()
	if err := ValidateQuantileSketchSnapshot(candidate.Snapshot()); err != nil {
		return err
	}
	*sketch = candidate
	return nil
}

func mergeQuantileSketchSummaries(left, right []QuantileSketchSample) []QuantileSketchSample {
	merged := make([]QuantileSketchSample, 0, len(left)+len(right))
	leftIndex, rightIndex := 0, 0
	leftSpanIndex, rightSpanIndex := 0, 0
	var leftSpan, rightSpan uint64
	for leftIndex < len(left) || rightIndex < len(right) {
		if rightIndex == len(right) || leftIndex < len(left) && left[leftIndex].Value <= right[rightIndex].Value {
			sample := left[leftIndex]
			leftIndex++
			for rightSpanIndex < len(right) && right[rightSpanIndex].Value <= sample.Value {
				if right[rightSpanIndex].Delta > rightSpan {
					rightSpan = right[rightSpanIndex].Delta
				}
				rightSpanIndex++
			}
			sample.Delta = saturatingAddUint64Quantile(sample.Delta, rightSpan)
			merged = append(merged, sample)
			continue
		}
		sample := right[rightIndex]
		rightIndex++
		for leftSpanIndex < len(left) && left[leftSpanIndex].Value <= sample.Value {
			if left[leftSpanIndex].Delta > leftSpan {
				leftSpan = left[leftSpanIndex].Delta
			}
			leftSpanIndex++
		}
		sample.Delta = saturatingAddUint64Quantile(sample.Delta, leftSpan)
		merged = append(merged, sample)
	}
	return merged
}
