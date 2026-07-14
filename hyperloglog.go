package hatriecache

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	mathbits "math/bits"
	"strconv"
)

const (
	DefaultHyperLogLogPrecision uint8 = 14
	minHyperLogLogPrecision     uint8 = 4
	maxHyperLogLogPrecision     uint8 = 20
)

// HyperLogLogInfo reports the shape and current estimate of a HyperLogLog.
// HyperLogLog stores approximate distinct counts without storing observed items.
type HyperLogLogInfo struct {
	Precision        uint8  `json:"precision"`
	RegisterCount    uint64 `json:"register_count"`
	RegisterBytes    uint64 `json:"register_bytes"`
	Observations     uint64 `json:"observations"`
	NonZeroRegisters uint64 `json:"non_zero_registers"`
	Estimate         uint64 `json:"estimate"`
}

type hyperLogLogSnapshot struct {
	Precision    uint8  `json:"precision"`
	Observations uint64 `json:"observations"`
	Registers    string `json:"registers"`
}

type hyperLogLogData struct {
	registers    []uint8
	precision    uint8
	observations uint64
}

func newHyperLogLogData(precision uint8) (hyperLogLogData, error) {
	if err := validateHyperLogLogPrecision(precision); err != nil {
		return hyperLogLogData{}, err
	}
	return hyperLogLogData{
		registers: make([]uint8, hyperLogLogRegisterCount(precision)),
		precision: precision,
	}, nil
}

func newDefaultHyperLogLogData() hyperLogLogData {
	data, err := newHyperLogLogData(DefaultHyperLogLogPrecision)
	if err != nil {
		panic(err)
	}
	return data
}

func validateHyperLogLogPrecision(precision uint8) error {
	if precision < minHyperLogLogPrecision || precision > maxHyperLogLogPrecision {
		return errors.New("hatriecache: hyperloglog precision must be between " + strconv.Itoa(int(minHyperLogLogPrecision)) + " and " + strconv.Itoa(int(maxHyperLogLogPrecision)))
	}
	return nil
}

func validateHyperLogLogSnapshot(snapshot hyperLogLogSnapshot) error {
	if err := validateHyperLogLogPrecision(snapshot.Precision); err != nil {
		return err
	}
	data, err := base64.StdEncoding.DecodeString(snapshot.Registers)
	if err != nil {
		return err
	}
	if len(data) != hyperLogLogRegisterCount(snapshot.Precision) {
		return errors.New("hatriecache: invalid hyperloglog register length")
	}
	maxRank := hyperLogLogMaxRank(snapshot.Precision)
	nonZeroRegisters := uint64(0)
	for _, register := range data {
		if register > maxRank {
			return errors.New("hatriecache: invalid hyperloglog register rank")
		}
		if register != 0 {
			nonZeroRegisters++
		}
	}
	if nonZeroRegisters > snapshot.Observations {
		return errors.New("hatriecache: hyperloglog snapshot has more nonzero registers than observations")
	}
	return nil
}

func newHyperLogLogDataFromSnapshot(snapshot hyperLogLogSnapshot) (hyperLogLogData, error) {
	if err := validateHyperLogLogSnapshot(snapshot); err != nil {
		return hyperLogLogData{}, err
	}
	raw, err := base64.StdEncoding.DecodeString(snapshot.Registers)
	if err != nil {
		return hyperLogLogData{}, err
	}
	registers := make([]uint8, len(raw))
	copy(registers, raw)
	return hyperLogLogData{
		registers:    registers,
		precision:    snapshot.Precision,
		observations: snapshot.Observations,
	}, nil
}

func (hll *hyperLogLogData) Add(value interface{}) bool {
	changed, _ := hll.AddChecked(value)
	return changed
}

func (hll *hyperLogLogData) AddChecked(value interface{}) (bool, error) {
	changed, err := hll.AddOneChecked(value)
	return changed > 0, err
}

func (hll *hyperLogLogData) AddOne(value interface{}, values ...interface{}) int {
	changed, _ := hll.AddOneChecked(value, values...)
	return changed
}

func (hll *hyperLogLogData) AddOneChecked(value interface{}, values ...interface{}) (int, error) {
	if hll == nil || hll.precision == 0 || len(hll.registers) == 0 {
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

func (hll *hyperLogLogData) addKey(key []byte) bool {
	index, rank := hyperLogLogIndexAndRank(bloomFilterFNV64a(key), hll.precision)
	hll.observations = saturatingAddUint64(hll.observations, 1)
	if rank <= hll.registers[index] {
		return false
	}
	hll.registers[index] = rank
	return true
}

func (hll hyperLogLogData) Count() uint64 {
	return hyperLogLogEstimateUint64(hll.estimate())
}

func (hll hyperLogLogData) Info() HyperLogLogInfo {
	var nonZero uint64
	for _, register := range hll.registers {
		if register != 0 {
			nonZero++
		}
	}
	return HyperLogLogInfo{
		Precision:        hll.precision,
		RegisterCount:    uint64(len(hll.registers)),
		RegisterBytes:    uint64(len(hll.registers)),
		Observations:     hll.observations,
		NonZeroRegisters: nonZero,
		Estimate:         hll.Count(),
	}
}

func (hll hyperLogLogData) Snapshot() hyperLogLogSnapshot {
	return hyperLogLogSnapshot{
		Precision:    hll.precision,
		Observations: hll.observations,
		Registers:    base64.StdEncoding.EncodeToString(hll.registers),
	}
}

func (hll hyperLogLogData) EncodedSize() int64 {
	return int64(len(hll.registers))
}

func (hll hyperLogLogData) estimate() float64 {
	m := float64(len(hll.registers))
	if m == 0 {
		return 0
	}
	sum := 0.0
	zeros := 0
	for _, register := range hll.registers {
		if register == 0 {
			zeros++
		}
		sum += math.Ldexp(1, -int(register))
	}
	if sum == 0 {
		return 0
	}
	raw := hyperLogLogAlpha(m) * m * m / sum
	if raw <= 2.5*m && zeros > 0 {
		return m * math.Log(m/float64(zeros))
	}
	const two64 = 18446744073709551616.0
	if raw > two64/30 {
		corrected := -two64 * math.Log1p(-raw/two64)
		if !math.IsInf(corrected, 0) && !math.IsNaN(corrected) && corrected > 0 {
			return corrected
		}
	}
	return raw
}

func hyperLogLogAlpha(m float64) float64 {
	switch int(m) {
	case 16:
		return 0.673
	case 32:
		return 0.697
	case 64:
		return 0.709
	default:
		return 0.7213 / (1 + 1.079/m)
	}
}

func hyperLogLogEstimateUint64(value float64) uint64 {
	if value <= 0 || math.IsNaN(value) {
		return 0
	}
	if value >= float64(^uint64(0)) {
		return ^uint64(0)
	}
	return uint64(value + 0.5)
}

func hyperLogLogRegisterCount(precision uint8) int {
	return 1 << precision
}

func hyperLogLogMaxRank(precision uint8) uint8 {
	return 64 - precision + 1
}

func hyperLogLogIndexAndRank(hash uint64, precision uint8) (int, uint8) {
	mask := uint64(1<<precision) - 1
	index := int(hash & mask)
	remaining := hash >> precision
	if remaining == 0 {
		return index, hyperLogLogMaxRank(precision)
	}
	rank := mathbits.LeadingZeros64(remaining) - int(precision) + 1
	if rank < 1 {
		rank = 1
	}
	maxRank := int(hyperLogLogMaxRank(precision))
	if rank > maxRank {
		rank = maxRank
	}
	return index, uint8(rank)
}

func hyperLogLogItemKeys(value interface{}, values ...interface{}) ([][]byte, error) {
	keys := make([][]byte, 0, 1+len(values))
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

// HyperLogLogStorage stores HyperLogLog values outside the trie.
type HyperLogLogStorage struct {
	array     []hyperLogLogData
	reusables reusableIndexes
}

func CreateHyperLogLogStorage() *HyperLogLogStorage {
	return &HyperLogLogStorage{
		array: []hyperLogLogData{},
	}
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
	data, err := newHyperLogLogData(precision)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertReplacementLocation(key)
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
