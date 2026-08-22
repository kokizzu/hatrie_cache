package hatDataStructure

import (
	"errors"
	"math"
	"sort"
	"strconv"

	json "github.com/goccy/go-json"
)

const (
	DefaultQuantileSketchEpsilon = 0.01
	minQuantileSketchEpsilon     = 0.0001
	maxQuantileSketchEpsilon     = 0.5
	MinQuantileSketchEpsilon     = minQuantileSketchEpsilon
	MaxQuantileSketchEpsilon     = maxQuantileSketchEpsilon
)

// QuantileEstimate is an approximate value for a requested quantile.
type QuantileEstimate struct {
	Quantile  float64 `json:"quantile"`
	Value     float64 `json:"value"`
	Count     uint64  `json:"count"`
	RankError uint64  `json:"rank_error"`
}

// QuantileSketchInfo reports the size and bounds of a compact quantile sketch.
type QuantileSketchInfo struct {
	Epsilon      float64 `json:"epsilon"`
	Count        uint64  `json:"count"`
	SummarySize  uint64  `json:"summary_size"`
	Min          float64 `json:"min"`
	Max          float64 `json:"max"`
	RankError    uint64  `json:"rank_error"`
	EncodedBytes int64   `json:"encoded_bytes"`
}

// QuantileSketchSample is one compressed summary entry.
type QuantileSketchSample struct {
	Value float64 `json:"value"`
	Gap   uint64  `json:"gap"`
	Delta uint64  `json:"delta"`
}

// QuantileSketchSnapshot is a portable summary representation.
type QuantileSketchSnapshot struct {
	Epsilon float64                `json:"epsilon"`
	Count   uint64                 `json:"count"`
	Summary []QuantileSketchSample `json:"summary"`
}

// QuantileSketch tracks approximate quantiles using bounded rank error.
type QuantileSketch struct {
	epsilon float64
	count   uint64
	summary []QuantileSketchSample
}

func NewQuantileSketch(epsilon float64) (QuantileSketch, error) {
	if err := ValidateQuantileSketchEpsilon(epsilon); err != nil {
		return QuantileSketch{}, err
	}
	return QuantileSketch{epsilon: epsilon}, nil
}

func NewDefaultQuantileSketch() QuantileSketch {
	sketch, err := NewQuantileSketch(DefaultQuantileSketchEpsilon)
	if err != nil {
		panic(err)
	}
	return sketch
}

func ValidateQuantileSketchEpsilon(epsilon float64) error {
	if math.IsNaN(epsilon) || math.IsInf(epsilon, 0) || epsilon < minQuantileSketchEpsilon || epsilon > maxQuantileSketchEpsilon {
		return errors.New("hatriecache: quantile sketch epsilon must be between " + strconv.FormatFloat(minQuantileSketchEpsilon, 'f', -1, 64) + " and " + strconv.FormatFloat(maxQuantileSketchEpsilon, 'f', -1, 64))
	}
	return nil
}

func ValidateQuantileSketchSnapshot(snapshot QuantileSketchSnapshot) error {
	if err := ValidateQuantileSketchEpsilon(snapshot.Epsilon); err != nil {
		return err
	}
	if snapshot.Count == 0 {
		if len(snapshot.Summary) != 0 {
			return errors.New("hatriecache: empty quantile sketch snapshot must not contain summary samples")
		}
		return nil
	}
	if len(snapshot.Summary) == 0 {
		return errors.New("hatriecache: quantile sketch snapshot summary is required")
	}
	if uint64(len(snapshot.Summary)) > snapshot.Count {
		return errors.New("hatriecache: quantile sketch snapshot has too many samples")
	}
	rankAllowance := quantileSketchRankAllowance(snapshot.Epsilon, snapshot.Count)
	var gapTotal uint64
	for idx, sample := range snapshot.Summary {
		if !IsFiniteQuantileValue(sample.Value) {
			return errors.New("hatriecache: quantile sketch sample must be a finite number")
		}
		if sample.Gap == 0 {
			return errors.New("hatriecache: quantile sketch sample gap must be positive")
		}
		if idx > 0 && sample.Value < snapshot.Summary[idx-1].Value {
			return errors.New("hatriecache: quantile sketch samples must be sorted")
		}
		if (idx == 0 || idx == len(snapshot.Summary)-1) && sample.Delta != 0 {
			return errors.New("hatriecache: quantile sketch boundary sample delta must be zero")
		}
		if sample.Delta > snapshot.Count || sample.Gap > snapshot.Count-gapTotal {
			return errors.New("hatriecache: quantile sketch sample rank metadata is invalid")
		}
		if sample.Delta > rankAllowance || sample.Gap > rankAllowance-sample.Delta {
			return errors.New("hatriecache: quantile sketch sample rank span exceeds allowed error")
		}
		gapTotal += sample.Gap
	}
	if gapTotal != snapshot.Count {
		return errors.New("hatriecache: quantile sketch sample gaps do not match count")
	}
	return nil
}

func NewQuantileSketchFromSnapshot(snapshot QuantileSketchSnapshot) (QuantileSketch, error) {
	if err := ValidateQuantileSketchSnapshot(snapshot); err != nil {
		return QuantileSketch{}, err
	}
	sketch := QuantileSketch{
		epsilon: snapshot.Epsilon,
		count:   snapshot.Count,
		summary: make([]QuantileSketchSample, len(snapshot.Summary)),
	}
	copy(sketch.summary, snapshot.Summary)
	return sketch, nil
}

func (sketch *QuantileSketch) Add(value float64, values ...float64) QuantileEstimate {
	if sketch == nil || sketch.epsilon == 0 || !validQuantileSketchValues(value, values...) {
		return QuantileEstimate{}
	}
	sketch.addValid(value)
	for _, value := range values {
		sketch.addValid(value)
	}
	estimate, _ := sketch.Estimate(0.5)
	return estimate
}

// AddValidBatch adds values that have already been checked as finite.
func (sketch *QuantileSketch) AddValidBatch(values []float64) QuantileEstimate {
	if sketch == nil || sketch.epsilon == 0 {
		return QuantileEstimate{}
	}
	for _, value := range values {
		sketch.addValid(value)
	}
	estimate, _ := sketch.Estimate(0.5)
	return estimate
}

func (sketch *QuantileSketch) addValid(value float64) {
	sketch.count = saturatingAddUint64Quantile(sketch.count, 1)
	insert := sort.Search(len(sketch.summary), func(idx int) bool {
		return sketch.summary[idx].Value > value
	})
	delta := uint64(0)
	if insert > 0 && insert < len(sketch.summary) {
		if allowance := sketch.rankAllowance(); allowance > 0 {
			delta = allowance - 1
		}
	}
	sample := QuantileSketchSample{Value: value, Gap: 1, Delta: delta}
	sketch.summary = append(sketch.summary, QuantileSketchSample{})
	copy(sketch.summary[insert+1:], sketch.summary[insert:])
	sketch.summary[insert] = sample
	sketch.compress()
}

func (sketch QuantileSketch) Estimate(quantile float64) (QuantileEstimate, bool) {
	if len(sketch.summary) == 0 || sketch.count == 0 {
		return QuantileEstimate{}, false
	}
	if quantile <= 0 {
		return sketch.estimateFromValue(0, sketch.summary[0].Value), true
	}
	last := sketch.summary[len(sketch.summary)-1]
	if quantile >= 1 {
		return sketch.estimateFromValue(1, last.Value), true
	}

	target := quantile * float64(sketch.count)
	allowed := sketch.epsilon * float64(sketch.count)
	rankMin := uint64(0)
	previous := sketch.summary[0]
	for _, sample := range sketch.summary {
		rankMin = saturatingAddUint64Quantile(rankMin, sample.Gap)
		if float64(saturatingAddUint64Quantile(rankMin, sample.Delta)) > target+allowed {
			return sketch.estimateFromValue(quantile, previous.Value), true
		}
		previous = sample
	}
	return sketch.estimateFromValue(quantile, last.Value), true
}

func (sketch QuantileSketch) Snapshot() QuantileSketchSnapshot {
	out := QuantileSketchSnapshot{
		Epsilon: sketch.epsilon,
		Count:   sketch.count,
		Summary: make([]QuantileSketchSample, len(sketch.summary)),
	}
	copy(out.Summary, sketch.summary)
	return out
}

func (sketch QuantileSketch) Info() QuantileSketchInfo {
	info := QuantileSketchInfo{Epsilon: sketch.epsilon, Count: sketch.count, SummarySize: uint64(len(sketch.summary)), RankError: sketch.rankError()}
	if len(sketch.summary) > 0 {
		info.Min = sketch.summary[0].Value
		info.Max = sketch.summary[len(sketch.summary)-1].Value
	}
	info.EncodedBytes = sketch.EncodedSize()
	return info
}

func (sketch QuantileSketch) EncodedSize() int64 {
	data, err := json.Marshal(sketch.Snapshot())
	if err != nil {
		return 0
	}
	return int64(len(data))
}

func (sketch *QuantileSketch) compress() {
	if len(sketch.summary) < 3 {
		return
	}
	allowance := sketch.rankAllowance()
	if allowance == 0 {
		return
	}
	for idx := len(sketch.summary) - 2; idx > 0; idx-- {
		current, next := sketch.summary[idx], sketch.summary[idx+1]
		if saturatingAddUint64Quantile(saturatingAddUint64Quantile(current.Gap, next.Gap), next.Delta) > allowance {
			continue
		}
		sketch.summary[idx+1].Gap = saturatingAddUint64Quantile(next.Gap, current.Gap)
		copy(sketch.summary[idx:], sketch.summary[idx+1:])
		sketch.summary = sketch.summary[:len(sketch.summary)-1]
	}
}

func (sketch QuantileSketch) estimateFromValue(quantile float64, value float64) QuantileEstimate {
	return QuantileEstimate{Quantile: quantile, Value: value, Count: sketch.count, RankError: sketch.rankError()}
}

func (sketch QuantileSketch) rankAllowance() uint64 {
	return quantileSketchRankAllowance(sketch.epsilon, sketch.count)
}

func (sketch QuantileSketch) rankError() uint64 {
	if sketch.count == 0 {
		return 0
	}
	err := uint64(math.Ceil(sketch.epsilon * float64(sketch.count)))
	if err < 1 {
		return 1
	}
	return err
}

func quantileSketchRankAllowance(epsilon float64, count uint64) uint64 {
	if count == 0 {
		return 0
	}
	allowance := uint64(math.Floor(2 * epsilon * float64(count)))
	if allowance < 1 {
		return 1
	}
	return allowance
}

// IsFiniteQuantileValue reports whether value can be stored in a sketch.
func IsFiniteQuantileValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validQuantileSketchValues(value float64, values ...float64) bool {
	if !IsFiniteQuantileValue(value) {
		return false
	}
	for _, value := range values {
		if !IsFiniteQuantileValue(value) {
			return false
		}
	}
	return true
}

func saturatingAddUint64Quantile(value uint64, delta uint64) uint64 {
	if ^uint64(0)-value < delta {
		return ^uint64(0)
	}
	return value + delta
}
