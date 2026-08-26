package hatCache

import (
	"fmt"

	json "github.com/goccy/go-json"
	"hatrie_cache/hat/hatDataStructure"
)

const (
	DefaultHyperLogLogPrecision = hatDataStructure.DefaultHyperLogLogPrecision
	minHyperLogLogPrecision     = hatDataStructure.MinHyperLogLogPrecision
	maxHyperLogLogPrecision     = hatDataStructure.MaxHyperLogLogPrecision
)

type HyperLogLogInfo = hatDataStructure.HyperLogLogInfo
type hyperLogLogSnapshot = hatDataStructure.HyperLogLogSnapshot
type hyperLogLogData struct{ hll hatDataStructure.HyperLogLog }

func newHyperLogLogData(precision uint8) (hyperLogLogData, error) {
	hll, err := hatDataStructure.NewHyperLogLog(precision)
	return hyperLogLogData{hll: hll}, err
}
func newDefaultHyperLogLogData() hyperLogLogData {
	return hyperLogLogData{hll: hatDataStructure.NewDefaultHyperLogLog()}
}
func validateHyperLogLogPrecision(precision uint8) error {
	return hatDataStructure.ValidateHyperLogLogPrecision(precision)
}
func validateHyperLogLogSnapshot(snapshot hyperLogLogSnapshot) error {
	return hatDataStructure.ValidateHyperLogLogSnapshot(snapshot)
}
func hyperLogLogRegisterCount(precision uint8) int {
	return hatDataStructure.HyperLogLogRegisterCount(precision)
}
func hyperLogLogMaxRank(precision uint8) uint8 { return hatDataStructure.HyperLogLogMaxRank(precision) }
func hyperLogLogAlpha(m float64) float64       { return hatDataStructure.HyperLogLogAlpha(m) }
func newHyperLogLogDataFromSnapshot(snapshot hyperLogLogSnapshot) (hyperLogLogData, error) {
	hll, err := hatDataStructure.NewHyperLogLogFromSnapshot(snapshot)
	return hyperLogLogData{hll: hll}, err
}
func (hll *hyperLogLogData) Add(value interface{}) bool {
	changed, _ := hll.AddChecked(value)
	return changed
}
func (hll *hyperLogLogData) AddChecked(value interface{}) (bool, error) { return hll.addChecked(value) }
func (hll *hyperLogLogData) AddOne(value interface{}, values ...interface{}) int {
	changed, _ := hll.AddOneChecked(value, values...)
	return changed
}
func (hll *hyperLogLogData) AddOneChecked(value interface{}, values ...interface{}) (int, error) {
	if hll == nil || hll.hll.Precision() == 0 {
		return 0, nil
	}
	keys, err := hyperLogLogItemKeys(value, values...)
	if err != nil {
		return 0, err
	}
	changed := 0
	for _, key := range keys {
		if hll.addKey(key) {
			changed++
		}
	}
	return changed, nil
}
func (hll *hyperLogLogData) addCommandBatch(values Slice) error {
	if hll == nil || hll.hll.Precision() == 0 {
		return nil
	}
	if len(values) == 1 {
		if text, ok := values[0].(string); ok && commandFastCanonicalJSONString(text) {
			hll.addJSONString(text)
			return nil
		}
		key, err := hyperLogLogItemKey(values[0])
		if err != nil {
			return err
		}
		hll.addKey(key)
		return nil
	}
	var encoded [][]byte
	for index, value := range values {
		text, ok := value.(string)
		if ok && commandFastCanonicalJSONString(text) {
			continue
		}
		if encoded == nil {
			encoded = make([][]byte, len(values))
		}
		key, err := hyperLogLogItemKey(value)
		if err != nil {
			return err
		}
		encoded[index] = key
	}
	if encoded == nil {
		for _, value := range values {
			hll.addJSONString(value.(string))
		}
		return nil
	}
	for index, value := range values {
		if encoded[index] != nil {
			hll.addKey(encoded[index])
		} else {
			hll.addJSONString(value.(string))
		}
	}
	return nil
}
func (hll *hyperLogLogData) addKey(key []byte) bool { return hll != nil && hll.hll.AddBytes(key) == 1 }
func (hll *hyperLogLogData) addChecked(value interface{}) (bool, error) {
	if hll == nil || hll.hll.Precision() == 0 {
		return false, nil
	}
	key, err := hyperLogLogItemKey(value)
	if err != nil {
		return false, err
	}
	return hll.addKey(key), nil
}
func (hll *hyperLogLogData) addJSONString(value string) bool {
	return hll != nil && hll.hll.AddJSONString(value)
}
func (hll hyperLogLogData) Count() uint64                 { return hll.hll.Count() }
func (hll hyperLogLogData) Info() HyperLogLogInfo         { return hll.hll.Info() }
func (hll hyperLogLogData) Snapshot() hyperLogLogSnapshot { return hll.hll.Snapshot() }
func (hll hyperLogLogData) EncodedSize() int64            { return hll.hll.EncodedSize() }
func hyperLogLogItemKeys(value interface{}, values ...interface{}) ([][]byte, error) {
	count, ok := checkedBatchSize(1, len(values))
	if !ok {
		return nil, errBatchSizeTooLarge
	}
	keys := make([][]byte, 0, count)
	key, err := hyperLogLogItemKey(value)
	if err != nil {
		return nil, err
	}
	keys = append(keys, key)
	for _, value := range values {
		key, err := hyperLogLogItemKey(value)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}
func hyperLogLogItemKey(value interface{}) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("hatriecache: unsupported hyperloglog value: %w", err)
	}
	return data, nil
}

type HyperLogLogStorage struct {
	array     []hyperLogLogData
	reusables reusableIndexes
}

func CreateHyperLogLogStorage() *HyperLogLogStorage {
	return &HyperLogLogStorage{array: []hyperLogLogData{}}
}
func (store *HyperLogLogStorage) PutData(idx int32, value hyperLogLogData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}
func (store *HyperLogLogStorage) AppendData(value hyperLogLogData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}
func (store *HyperLogLogStorage) AddData(value hyperLogLogData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}
func (store *HyperLogLogStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = hyperLogLogData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}
func (ht *HatTrie) UpsertHyperLogLog(key string, precision uint8) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertHyperLogLog(key, precision)
	}
	data, err := newHyperLogLogData(precision)
	if err != nil {
		return err
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsHyperLogLog() {
		ht.hyperLogLogs.PutData(hval.Index, data)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.hyperLogLogs.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_HYPERLOGLOG}.toValue()
	ht.recordWriteLocked(key)
	return nil
}
func (ht *HatTrie) AddHyperLogLog(key string, val interface{}, vals ...interface{}) uint64 {
	estimate, _ := ht.AddHyperLogLogChecked(key, val, vals...)
	return estimate
}
func (ht *HatTrie) AddHyperLogLogChecked(key string, val interface{}, vals ...interface{}) (uint64, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.AddHyperLogLogChecked(key, val, vals...)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return 0, err
	}
	if hval.IsHyperLogLog() {
		if _, err := ht.hyperLogLogs.array[hval.Index].AddOneChecked(val, vals...); err != nil {
			return 0, err
		}
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return ht.hyperLogLogs.array[hval.Index].Count(), nil
	}
	data := newDefaultHyperLogLogData()
	if _, err := data.AddOneChecked(val, vals...); err != nil {
		return 0, err
	}
	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.hyperLogLogs.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_HYPERLOGLOG}.toValue()
	ht.recordWriteLocked(key)
	return ht.hyperLogLogs.array[idx].Count(), nil
}
func (ht *HatTrie) CountHyperLogLog(key string) (uint64, bool) {
	count, ok, _ := ht.CountHyperLogLogChecked(key)
	return count, ok
}
func (ht *HatTrie) CountHyperLogLogChecked(key string) (uint64, bool, error) {
	if ht == nil {
		return 0, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.CountHyperLogLogChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, false, err
	}
	if !hval.IsHyperLogLog() {
		ht.recordReadLocked(false, key)
		return 0, false, nil
	}
	count := ht.hyperLogLogs.array[hval.Index].Count()
	ht.recordReadLocked(true, key)
	return count, true, nil
}
func (ht *HatTrie) HyperLogLogInfo(key string) (HyperLogLogInfo, bool) {
	info, ok, _ := ht.HyperLogLogInfoChecked(key)
	return info, ok
}
func (ht *HatTrie) HyperLogLogInfoChecked(key string) (HyperLogLogInfo, bool, error) {
	if ht == nil {
		return HyperLogLogInfo{}, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.HyperLogLogInfoChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return HyperLogLogInfo{}, false, err
	}
	if !hval.IsHyperLogLog() {
		ht.recordReadLocked(false, key)
		return HyperLogLogInfo{}, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.hyperLogLogs.array[hval.Index].Info(), true, nil
}
