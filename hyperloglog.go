package hatriecache

import (
	"encoding/base64"
	"encoding/json"
	"errors"
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
	for _, register := range data {
		if register > maxRank {
			return errors.New("hatriecache: invalid hyperloglog register rank")
		}
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
	if hll == nil || hll.precision == 0 || len(hll.registers) == 0 {
		return false
	}
	key := mustHyperLogLogItemKey(value)
	index, rank := hyperLogLogIndexAndRank(bloomFilterFNV64a(key), hll.precision)
	hll.observations = saturatingAddUint64(hll.observations, 1)
	if rank <= hll.registers[index] {
		return false
	}
	hll.registers[index] = rank
	return true
}

func (hll *hyperLogLogData) AddOne(value interface{}, values ...interface{}) int {
	changed := 0
	if hll.Add(value) {
		changed++
	}
	for _, value := range values {
		if hll.Add(value) {
			changed++
		}
	}
	return changed
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

func mustHyperLogLogItemKey(value interface{}) []byte {
	key, err := hyperLogLogItemKey(value)
	if err != nil {
		panic(err)
	}
	return key
}

func hyperLogLogItemKey(value interface{}) ([]byte, error) {
	return json.Marshal(value)
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

	rawPtr, hval := ht.upsertFreshLocation(key)
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
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsHyperLogLog() {
		ht.hyperLogLogs.array[hval.Index].AddOne(val, vals...)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return ht.hyperLogLogs.array[hval.Index].Count()
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.hyperLogLogs.AddData(newDefaultHyperLogLogData())
	ht.hyperLogLogs.array[idx].AddOne(val, vals...)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_HYPERLOGLOG}.toValue()
	ht.recordWriteLocked(key)
	return ht.hyperLogLogs.array[idx].Count()
}

func (ht *HatTrie) CountHyperLogLog(key string) (uint64, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsHyperLogLog() {
		ht.recordReadLocked(false, key)
		return 0, false
	}
	count := ht.hyperLogLogs.array[hval.Index].Count()
	ht.recordReadLocked(true, key)
	return count, true
}

func (ht *HatTrie) HyperLogLogInfo(key string) (HyperLogLogInfo, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsHyperLogLog() {
		ht.recordReadLocked(false, key)
		return HyperLogLogInfo{}, false
	}
	ht.recordReadLocked(true, key)
	return ht.hyperLogLogs.array[hval.Index].Info(), true
}
