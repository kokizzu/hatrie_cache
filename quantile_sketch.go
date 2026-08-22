package hatriecache

import (
	"errors"
	"math"

	"hatrie_cache/hat/hatDataStructure"
)

const (
	DefaultQuantileSketchEpsilon = hatDataStructure.DefaultQuantileSketchEpsilon
	minQuantileSketchEpsilon     = hatDataStructure.MinQuantileSketchEpsilon
	maxQuantileSketchEpsilon     = hatDataStructure.MaxQuantileSketchEpsilon
)

// QuantileEstimate is an approximate value for a requested quantile.
type QuantileEstimate = hatDataStructure.QuantileEstimate

// QuantileSketchInfo reports the size and bounds of a compact quantile sketch.
type QuantileSketchInfo = hatDataStructure.QuantileSketchInfo

type quantileSketchSample = hatDataStructure.QuantileSketchSample

// quantileSketchSnapshot remains the cache snapshot schema.
type quantileSketchSnapshot struct {
	Epsilon float64                `json:"epsilon"`
	Count   uint64                 `json:"count"`
	Summary []quantileSketchSample `json:"summary"`
}

type quantileSketchData struct {
	sketch hatDataStructure.QuantileSketch
}

func newQuantileSketchData(epsilon float64) (quantileSketchData, error) {
	sketch, err := hatDataStructure.NewQuantileSketch(epsilon)
	return quantileSketchData{sketch: sketch}, err
}

func newDefaultQuantileSketchData() quantileSketchData {
	return quantileSketchData{sketch: hatDataStructure.NewDefaultQuantileSketch()}
}

func validateQuantileSketchEpsilon(epsilon float64) error {
	return hatDataStructure.ValidateQuantileSketchEpsilon(epsilon)
}

func validateQuantileSketchSnapshot(snapshot quantileSketchSnapshot) error {
	return hatDataStructure.ValidateQuantileSketchSnapshot(quantileSketchSnapshotToPublic(snapshot))
}

func newQuantileSketchDataFromSnapshot(snapshot quantileSketchSnapshot) (quantileSketchData, error) {
	sketch, err := hatDataStructure.NewQuantileSketchFromSnapshot(quantileSketchSnapshotToPublic(snapshot))
	return quantileSketchData{sketch: sketch}, err
}

func quantileSketchSnapshotToPublic(snapshot quantileSketchSnapshot) hatDataStructure.QuantileSketchSnapshot {
	return hatDataStructure.QuantileSketchSnapshot{Epsilon: snapshot.Epsilon, Count: snapshot.Count, Summary: snapshot.Summary}
}

func quantileSketchSnapshotFromPublic(snapshot hatDataStructure.QuantileSketchSnapshot) quantileSketchSnapshot {
	return quantileSketchSnapshot{Epsilon: snapshot.Epsilon, Count: snapshot.Count, Summary: snapshot.Summary}
}

func (sketch *quantileSketchData) Add(value float64, values ...float64) QuantileEstimate {
	if sketch == nil {
		return QuantileEstimate{}
	}
	return sketch.sketch.Add(value, values...)
}

func (sketch *quantileSketchData) addValidBatch(values []float64) QuantileEstimate {
	if sketch == nil {
		return QuantileEstimate{}
	}
	return sketch.sketch.AddValidBatch(values)
}

func (sketch quantileSketchData) Estimate(quantile float64) (QuantileEstimate, bool) {
	return sketch.sketch.Estimate(quantile)
}

func (sketch quantileSketchData) Snapshot() quantileSketchSnapshot {
	return quantileSketchSnapshotFromPublic(sketch.sketch.Snapshot())
}

func (sketch quantileSketchData) Info() QuantileSketchInfo {
	return sketch.sketch.Info()
}

func (sketch quantileSketchData) EncodedSize() int64 {
	return sketch.sketch.EncodedSize()
}

func validQuantileSketchValue(value float64) bool {
	return hatDataStructure.IsFiniteQuantileValue(value)
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
	if ht == nil {
		return ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertQuantileSketch(key, epsilon)
	}

	data, err := newQuantileSketchData(epsilon)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
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
	if ht == nil {
		return QuantileEstimate{}, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.AddQuantileSketchChecked(key, val, vals...)
	}
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

func (ht *HatTrie) addQuantileSketchCommandBatchChecked(key string, values []float64) (QuantileEstimate, error) {
	if ht == nil {
		return QuantileEstimate{}, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.addQuantileSketchCommandBatchChecked(key, values)
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return QuantileEstimate{}, err
	}
	if hval.IsQuantileSketch() {
		estimate := ht.quantileSketches.array[hval.Index].addValidBatch(values)
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
	estimate := ht.quantileSketches.array[idx].addValidBatch(values)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_QUANTILE_SKETCH}.toValue()
	ht.recordWriteLocked(key)
	return estimate, nil
}

func (ht *HatTrie) EstimateQuantileSketch(key string, quantile float64) (QuantileEstimate, bool) {
	estimate, ok, _ := ht.EstimateQuantileSketchChecked(key, quantile)
	return estimate, ok
}

func (ht *HatTrie) EstimateQuantileSketchChecked(key string, quantile float64) (QuantileEstimate, bool, error) {
	if ht == nil {
		return QuantileEstimate{}, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.EstimateQuantileSketchChecked(key, quantile)
	}
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
	if ht == nil {
		return QuantileSketchInfo{}, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.QuantileSketchInfoChecked(key)
	}

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
