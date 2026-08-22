package hatDataStructure

import (
	"encoding/base64"
	"errors"
	"math"
	mathbits "math/bits"
	"strconv"

	"hatrie_cache/hat/hatHash"
)

const (
	DefaultHyperLogLogPrecision uint8 = 14
	MinHyperLogLogPrecision     uint8 = 4
	MaxHyperLogLogPrecision     uint8 = 20
)

var hyperLogLogRankContributions = func() [65]float64 {
	var values [65]float64
	for rank := range values {
		values[rank] = math.Ldexp(1, -rank)
	}
	return values
}()

type HyperLogLogInfo struct {
	Precision        uint8  `json:"precision"`
	RegisterCount    uint64 `json:"register_count"`
	RegisterBytes    uint64 `json:"register_bytes"`
	Observations     uint64 `json:"observations"`
	NonZeroRegisters uint64 `json:"non_zero_registers"`
	Estimate         uint64 `json:"estimate"`
}
type HyperLogLogSnapshot struct {
	Precision    uint8  `json:"precision"`
	Observations uint64 `json:"observations"`
	Registers    string `json:"registers"`
}

// HyperLogLog estimates the cardinality of a byte stream without storing it.
type HyperLogLog struct {
	registers     []uint8
	observations  uint64
	harmonicSum   float64
	zeroRegisters uint32
	precision     uint8
}
type hyperLogLogData = HyperLogLog

func NewHyperLogLog(precision uint8) (HyperLogLog, error) {
	if err := ValidateHyperLogLogPrecision(precision); err != nil {
		return HyperLogLog{}, err
	}
	return HyperLogLog{precision: precision}, nil
}
func NewDefaultHyperLogLog() HyperLogLog {
	data, err := NewHyperLogLog(DefaultHyperLogLogPrecision)
	if err != nil {
		panic(err)
	}
	return data
}
func newHyperLogLogData(precision uint8) (hyperLogLogData, error) { return NewHyperLogLog(precision) }
func newDefaultHyperLogLogData() hyperLogLogData                  { return NewDefaultHyperLogLog() }
func ValidateHyperLogLogPrecision(precision uint8) error {
	if precision < MinHyperLogLogPrecision || precision > MaxHyperLogLogPrecision {
		return errors.New("hatriecache: hyperloglog precision must be between " + strconv.Itoa(int(MinHyperLogLogPrecision)) + " and " + strconv.Itoa(int(MaxHyperLogLogPrecision)))
	}
	return nil
}
func ValidateHyperLogLogSnapshot(snapshot HyperLogLogSnapshot) error {
	if err := ValidateHyperLogLogPrecision(snapshot.Precision); err != nil {
		return err
	}
	size, ok := hllBase64DecodedSize(snapshot.Registers)
	if !ok {
		return errors.New("hatriecache: invalid base64 encoding")
	}
	if size == 0 {
		if snapshot.Observations != 0 {
			return errors.New("hatriecache: empty hyperloglog registers have observations")
		}
		return nil
	}
	if size != hyperLogLogRegisterCount(snapshot.Precision) {
		return errors.New("hatriecache: invalid hyperloglog register length")
	}
	data, err := base64.StdEncoding.DecodeString(snapshot.Registers)
	if err != nil {
		return err
	}
	maxRank := hyperLogLogMaxRank(snapshot.Precision)
	nonZero := uint64(0)
	for _, register := range data {
		if register > maxRank {
			return errors.New("hatriecache: invalid hyperloglog register rank")
		}
		if register != 0 {
			nonZero++
		}
	}
	if nonZero > snapshot.Observations {
		return errors.New("hatriecache: hyperloglog snapshot has more nonzero registers than observations")
	}
	if snapshot.Observations > 0 && nonZero == 0 {
		return errors.New("hatriecache: observed hyperloglog snapshot has no registers")
	}
	return nil
}
func NewHyperLogLogFromSnapshot(snapshot HyperLogLogSnapshot) (HyperLogLog, error) {
	if err := ValidateHyperLogLogSnapshot(snapshot); err != nil {
		return HyperLogLog{}, err
	}
	raw, err := base64.StdEncoding.DecodeString(snapshot.Registers)
	if err != nil {
		return HyperLogLog{}, err
	}
	out := HyperLogLog{precision: snapshot.Precision, observations: snapshot.Observations}
	if len(raw) == 0 || (snapshot.Observations == 0 && !hyperLogLogRawHasRegisters(raw)) {
		return out, nil
	}
	out.registers = append([]uint8(nil), raw...)
	out.rebuildSummary()
	return out, nil
}
func newHyperLogLogDataFromSnapshot(snapshot HyperLogLogSnapshot) (hyperLogLogData, error) {
	return NewHyperLogLogFromSnapshot(snapshot)
}

// AddBytes observes byte values and returns how many changed a register.
func (hll *HyperLogLog) AddBytes(value []byte, values ...[]byte) int {
	if hll == nil || hll.precision == 0 {
		return 0
	}
	changed := 0
	if hll.addKey(value) {
		changed++
	}
	for _, value := range values {
		if hll.addKey(value) {
			changed++
		}
	}
	return changed
}

// AddJSONString observes a JSON string payload without allocating its encoded form.
func (hll *HyperLogLog) AddJSONString(value string) bool {
	if hll == nil || hll.precision == 0 {
		return false
	}
	hll.ensureRegisters()
	index, rank := hyperLogLogIndexAndRank(hatHash.FNV1a64JSONString(value), hll.precision)
	hll.observations = saturatingAddUint64HLL(hll.observations, 1)
	if rank <= hll.registers[index] {
		if hll.harmonicSum <= 0 {
			hll.rebuildSummary()
		}
		return false
	}
	hll.updateSummary(hll.registers[index], rank)
	hll.registers[index] = rank
	return true
}

func (hll *HyperLogLog) addJSONString(value string) bool { return hll.AddJSONString(value) }
func (hll *HyperLogLog) addKey(key []byte) bool {
	hll.ensureRegisters()
	index, rank := hyperLogLogIndexAndRank(hatHash.FNV1a64(key), hll.precision)
	hll.observations = saturatingAddUint64HLL(hll.observations, 1)
	if rank <= hll.registers[index] {
		if hll.harmonicSum <= 0 {
			hll.rebuildSummary()
		}
		return false
	}
	hll.updateSummary(hll.registers[index], rank)
	hll.registers[index] = rank
	return true
}
func (hll HyperLogLog) Count() uint64 { return hyperLogLogEstimateUint64(hll.estimate()) }
func (hll HyperLogLog) Info() HyperLogLogInfo {
	sum, zeros := hll.estimateState()
	nonZero := uint64(len(hll.registers) - zeros)
	return HyperLogLogInfo{Precision: hll.precision, RegisterCount: uint64(hll.logicalRegisterCount()), RegisterBytes: uint64(len(hll.registers)), Observations: hll.observations, NonZeroRegisters: nonZero, Estimate: hyperLogLogEstimateFromState(len(hll.registers), sum, zeros)}
}
func (hll HyperLogLog) Snapshot() HyperLogLogSnapshot {
	return HyperLogLogSnapshot{Precision: hll.precision, Observations: hll.observations, Registers: base64.StdEncoding.EncodeToString(hll.registers)}
}
func (hll HyperLogLog) EncodedSize() int64 { return int64(len(hll.registers)) }

// Precision returns the configured register precision.
func (hll HyperLogLog) Precision() uint8 { return hll.precision }

// Observations returns the saturated number of observed values.
func (hll HyperLogLog) Observations() uint64 { return hll.observations }

// RawRegisters returns the backing register bytes without copying. Callers must
// not retain or mutate the returned slice.
func (hll HyperLogLog) RawRegisters() []uint8 { return hll.registers }
func (hll HyperLogLog) logicalRegisterCount() int {
	if hll.precision == 0 {
		return 0
	}
	return hyperLogLogRegisterCount(hll.precision)
}
func (hll *HyperLogLog) ensureRegisters() {
	if hll != nil && len(hll.registers) == 0 && hll.precision > 0 {
		count := hyperLogLogRegisterCount(hll.precision)
		hll.registers = make([]uint8, count)
		hll.harmonicSum = float64(count)
		hll.zeroRegisters = uint32(count)
	}
}
func (hll HyperLogLog) estimate() float64 {
	sum, zeros := hll.estimateState()
	return hyperLogLogEstimateFloat64(len(hll.registers), sum, zeros)
}
func (hll HyperLogLog) estimateState() (float64, int) {
	if hll.summaryReady() {
		return hll.harmonicSum, int(hll.zeroRegisters)
	}
	return hyperLogLogFullScanState(hll.registers)
}
func hyperLogLogEstimateFromState(registers int, sum float64, zeros int) uint64 {
	return hyperLogLogEstimateUint64(hyperLogLogEstimateFloat64(registers, sum, zeros))
}
func hyperLogLogEstimateFloat64(registers int, sum float64, zeros int) float64 {
	m := float64(registers)
	if m == 0 || sum == 0 {
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
func hyperLogLogFullScanState(registers []uint8) (float64, int) {
	sum := 0.0
	zeros := 0
	for _, register := range registers {
		if register == 0 {
			zeros++
		}
		sum += hyperLogLogRankContribution(register)
	}
	return sum, zeros
}
func (hll *HyperLogLog) rebuildSummary() {
	if hll == nil || len(hll.registers) == 0 {
		return
	}
	sum, zeros := hyperLogLogFullScanState(hll.registers)
	hll.harmonicSum = sum
	hll.zeroRegisters = uint32(zeros)
}
func (hll *HyperLogLog) updateSummary(oldRank, newRank uint8) {
	if hll == nil || oldRank >= newRank {
		return
	}
	if hll.harmonicSum <= 0 {
		hll.rebuildSummary()
		if hll.harmonicSum <= 0 {
			return
		}
	}
	hll.harmonicSum += hyperLogLogRankContributions[newRank] - hyperLogLogRankContributions[oldRank]
	if oldRank == 0 && hll.zeroRegisters > 0 {
		hll.zeroRegisters--
	}
}
func hyperLogLogRankContribution(rank uint8) float64 {
	if int(rank) < len(hyperLogLogRankContributions) {
		return hyperLogLogRankContributions[rank]
	}
	return math.Ldexp(1, -int(rank))
}
func (hll HyperLogLog) summaryReady() bool {
	return len(hll.registers) > 0 && hll.harmonicSum > 0 && hll.zeroRegisters <= uint32(len(hll.registers))
}
func hyperLogLogAlpha(m float64) float64 {
	switch int(m) {
	case 16:
		return .673
	case 32:
		return .697
	case 64:
		return .709
	default:
		return .7213 / (1 + 1.079/m)
	}
}

// HyperLogLogAlpha returns the standard bias-correction coefficient.
func HyperLogLogAlpha(m float64) float64 { return hyperLogLogAlpha(m) }
func hyperLogLogEstimateUint64(value float64) uint64 {
	if value <= 0 || math.IsNaN(value) {
		return 0
	}
	if value >= float64(^uint64(0)) {
		return ^uint64(0)
	}
	return uint64(value + .5)
}
func hyperLogLogRegisterCount(precision uint8) int { return 1 << precision }

// HyperLogLogRegisterCount returns the register count for a valid precision.
func HyperLogLogRegisterCount(precision uint8) int { return hyperLogLogRegisterCount(precision) }
func hyperLogLogRawHasRegisters(data []byte) bool {
	for _, register := range data {
		if register != 0 {
			return true
		}
	}
	return false
}
func hyperLogLogMaxRank(precision uint8) uint8 { return 64 - precision + 1 }

// HyperLogLogMaxRank returns the largest valid register rank for precision.
func HyperLogLogMaxRank(precision uint8) uint8 { return hyperLogLogMaxRank(precision) }
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
func saturatingAddUint64HLL(value, delta uint64) uint64 {
	if ^uint64(0)-value < delta {
		return ^uint64(0)
	}
	return value + delta
}
func hllBase64DecodedSize(encoded string) (int, bool) {
	if len(encoded)%4 != 0 {
		return 0, false
	}
	padding := 0
	if len(encoded) >= 2 && encoded[len(encoded)-2:] == "==" {
		padding = 2
	} else if len(encoded) >= 1 && encoded[len(encoded)-1:] == "=" {
		padding = 1
	}
	return len(encoded)/4*3 - padding, true
}
