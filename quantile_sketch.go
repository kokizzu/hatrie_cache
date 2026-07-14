package hatriecache

import (
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
)

const (
	DefaultQuantileSketchEpsilon = 0.01
	minQuantileSketchEpsilon     = 0.0001
	maxQuantileSketchEpsilon     = 0.5
)

// QuantileEstimate is an approximate value for a requested quantile.
// RankError is the maximum rank error allowed by the sketch configuration.
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

type quantileSketchSnapshot struct {
	Epsilon float64                `json:"epsilon"`
	Count   uint64                 `json:"count"`
	Summary []quantileSketchSample `json:"summary"`
}

type quantileSketchSample struct {
	Value float64 `json:"value"`
	Gap   uint64  `json:"gap"`
	Delta uint64  `json:"delta"`
}

type quantileSketchData struct {
	epsilon float64
	count   uint64
	summary []quantileSketchSample
}

func newQuantileSketchData(epsilon float64) (quantileSketchData, error) {
	if err := validateQuantileSketchEpsilon(epsilon); err != nil {
		return quantileSketchData{}, err
	}
	return quantileSketchData{epsilon: epsilon}, nil
}

func newDefaultQuantileSketchData() quantileSketchData {
	data, err := newQuantileSketchData(DefaultQuantileSketchEpsilon)
	if err != nil {
		panic(err)
	}
	return data
}

func validateQuantileSketchEpsilon(epsilon float64) error {
	if math.IsNaN(epsilon) || math.IsInf(epsilon, 0) || epsilon < minQuantileSketchEpsilon || epsilon > maxQuantileSketchEpsilon {
		return errors.New("hatriecache: quantile sketch epsilon must be between " + strconv.FormatFloat(minQuantileSketchEpsilon, 'f', -1, 64) + " and " + strconv.FormatFloat(maxQuantileSketchEpsilon, 'f', -1, 64))
	}
	return nil
}

func validateQuantileSketchSnapshot(snapshot quantileSketchSnapshot) error {
	if err := validateQuantileSketchEpsilon(snapshot.Epsilon); err != nil {
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
	var gapTotal uint64
	for idx, sample := range snapshot.Summary {
		if !validQuantileSketchValue(sample.Value) {
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
		gapTotal += sample.Gap
	}
	if gapTotal != snapshot.Count {
		return errors.New("hatriecache: quantile sketch sample gaps do not match count")
	}
	return nil
}

func newQuantileSketchDataFromSnapshot(snapshot quantileSketchSnapshot) (quantileSketchData, error) {
	if err := validateQuantileSketchSnapshot(snapshot); err != nil {
		return quantileSketchData{}, err
	}
	data := quantileSketchData{
		epsilon: snapshot.Epsilon,
		count:   snapshot.Count,
		summary: make([]quantileSketchSample, len(snapshot.Summary)),
	}
	copy(data.summary, snapshot.Summary)
	return data, nil
}

func (sketch *quantileSketchData) Add(value float64, values ...float64) QuantileEstimate {
	if sketch == nil || sketch.epsilon == 0 {
		return QuantileEstimate{}
	}
	if !validQuantileSketchValues(value, values...) {
		return QuantileEstimate{}
	}
	sketch.addOne(value)
	for _, value := range values {
		sketch.addOne(value)
	}
	estimate, _ := sketch.Estimate(0.5)
	return estimate
}

func (sketch *quantileSketchData) addOne(value float64) {
	if !validQuantileSketchValue(value) {
		return
	}
	sketch.count = saturatingAddUint64(sketch.count, 1)
	insert := sort.Search(len(sketch.summary), func(idx int) bool {
		return sketch.summary[idx].Value > value
	})
	delta := uint64(0)
	if insert > 0 && insert < len(sketch.summary) {
		allowance := sketch.rankAllowance()
		if allowance > 0 {
			delta = allowance - 1
		}
	}
	sample := quantileSketchSample{Value: value, Gap: 1, Delta: delta}
	sketch.summary = append(sketch.summary, quantileSketchSample{})
	copy(sketch.summary[insert+1:], sketch.summary[insert:])
	sketch.summary[insert] = sample
	sketch.compress()
}

func (sketch quantileSketchData) Estimate(quantile float64) (QuantileEstimate, bool) {
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
		rankMin = saturatingAddUint64(rankMin, sample.Gap)
		if float64(saturatingAddUint64(rankMin, sample.Delta)) > target+allowed {
			return sketch.estimateFromValue(quantile, previous.Value), true
		}
		previous = sample
	}
	return sketch.estimateFromValue(quantile, last.Value), true
}

func (sketch quantileSketchData) Snapshot() quantileSketchSnapshot {
	out := make([]quantileSketchSample, len(sketch.summary))
	copy(out, sketch.summary)
	return quantileSketchSnapshot{
		Epsilon: sketch.epsilon,
		Count:   sketch.count,
		Summary: out,
	}
}

func (sketch quantileSketchData) Info() QuantileSketchInfo {
	info := QuantileSketchInfo{
		Epsilon:     sketch.epsilon,
		Count:       sketch.count,
		SummarySize: uint64(len(sketch.summary)),
		RankError:   sketch.rankError(),
	}
	if len(sketch.summary) > 0 {
		info.Min = sketch.summary[0].Value
		info.Max = sketch.summary[len(sketch.summary)-1].Value
	}
	info.EncodedBytes = sketch.EncodedSize()
	return info
}

func (sketch quantileSketchData) EncodedSize() int64 {
	data, err := json.Marshal(sketch.Snapshot())
	if err != nil {
		return 0
	}
	return int64(len(data))
}

func (sketch *quantileSketchData) compress() {
	if len(sketch.summary) < 3 {
		return
	}
	allowance := sketch.rankAllowance()
	if allowance == 0 {
		return
	}
	for idx := len(sketch.summary) - 2; idx > 0; idx-- {
		current := sketch.summary[idx]
		next := sketch.summary[idx+1]
		if saturatingAddUint64(saturatingAddUint64(current.Gap, next.Gap), next.Delta) > allowance {
			continue
		}
		sketch.summary[idx+1].Gap = saturatingAddUint64(next.Gap, current.Gap)
		copy(sketch.summary[idx:], sketch.summary[idx+1:])
		sketch.summary = sketch.summary[:len(sketch.summary)-1]
	}
}

func (sketch quantileSketchData) estimateFromValue(quantile float64, value float64) QuantileEstimate {
	return QuantileEstimate{
		Quantile:  quantile,
		Value:     value,
		Count:     sketch.count,
		RankError: sketch.rankError(),
	}
}

func (sketch quantileSketchData) rankAllowance() uint64 {
	if sketch.count == 0 {
		return 0
	}
	allowance := uint64(math.Floor(2 * sketch.epsilon * float64(sketch.count)))
	if allowance < 1 {
		return 1
	}
	return allowance
}

func (sketch quantileSketchData) rankError() uint64 {
	if sketch.count == 0 {
		return 0
	}
	err := uint64(math.Ceil(sketch.epsilon * float64(sketch.count)))
	if err < 1 {
		return 1
	}
	return err
}

func validQuantileSketchValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validQuantileSketchValues(value float64, values ...float64) bool {
	if !validQuantileSketchValue(value) {
		return false
	}
	for _, value := range values {
		if !validQuantileSketchValue(value) {
			return false
		}
	}
	return true
}

// QuantileSketchStorage stores compact quantile sketches outside the trie.
type QuantileSketchStorage struct {
	array     []quantileSketchData
	reusables reusableIndexes
}

func CreateQuantileSketchStorage() *QuantileSketchStorage {
	return &QuantileSketchStorage{
		array: []quantileSketchData{},
	}
}

func (store *QuantileSketchStorage) PutData(idx int32, value quantileSketchData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}

func (store *QuantileSketchStorage) AppendData(value quantileSketchData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}

func (store *QuantileSketchStorage) AddData(value quantileSketchData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}

func (store *QuantileSketchStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = quantileSketchData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertQuantileSketch(key string, epsilon float64) error {
	data, err := newQuantileSketchData(epsilon)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsQuantileSketch() {
		ht.quantileSketches.PutData(hval.Index, data)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.quantileSketches.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_QUANTILE_SKETCH}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) AddQuantileSketch(key string, val float64, vals ...float64) QuantileEstimate {
	estimate, _ := ht.AddQuantileSketchChecked(key, val, vals...)
	return estimate
}

func (ht *HatTrie) AddQuantileSketchChecked(key string, val float64, vals ...float64) (QuantileEstimate, error) {
	if !validQuantileSketchValues(val, vals...) {
		return QuantileEstimate{}, errors.New("hatriecache: quantile sketch values must be finite numbers")
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return QuantileEstimate{}, err
	}
	if hval.IsQuantileSketch() {
		estimate := ht.quantileSketches.array[hval.Index].Add(val, vals...)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return estimate, nil
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.quantileSketches.AddData(newDefaultQuantileSketchData())
	estimate := ht.quantileSketches.array[idx].Add(val, vals...)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_QUANTILE_SKETCH}.toValue()
	ht.recordWriteLocked(key)
	return estimate, nil
}

func (ht *HatTrie) EstimateQuantileSketch(key string, quantile float64) (QuantileEstimate, bool) {
	estimate, ok, _ := ht.EstimateQuantileSketchChecked(key, quantile)
	return estimate, ok
}

func (ht *HatTrie) EstimateQuantileSketchChecked(key string, quantile float64) (QuantileEstimate, bool, error) {
	if math.IsNaN(quantile) || math.IsInf(quantile, 0) || quantile < 0 || quantile > 1 {
		return QuantileEstimate{}, false, errors.New("hatriecache: quantile must be between 0 and 1")
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return QuantileEstimate{}, false, err
	}
	if !hval.IsQuantileSketch() {
		ht.recordReadLocked(false, key)
		return QuantileEstimate{}, false, nil
	}
	estimate, ok := ht.quantileSketches.array[hval.Index].Estimate(quantile)
	ht.recordReadLocked(ok, key)
	return estimate, ok, nil
}

func (ht *HatTrie) QuantileSketchInfo(key string) (QuantileSketchInfo, bool) {
	info, ok, _ := ht.QuantileSketchInfoChecked(key)
	return info, ok
}

func (ht *HatTrie) QuantileSketchInfoChecked(key string) (QuantileSketchInfo, bool, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return QuantileSketchInfo{}, false, err
	}
	if !hval.IsQuantileSketch() {
		ht.recordReadLocked(false, key)
		return QuantileSketchInfo{}, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.quantileSketches.array[hval.Index].Info(), true, nil
}

func quantileSketchEpsilonValue(value float64) (float64, error) {
	if err := validateQuantileSketchEpsilon(value); err != nil {
		return 0, err
	}
	return value, nil
}
