package hatDataStructure

import (
	"encoding/base64"
	"math"
	"strconv"
	"testing"
	"unsafe"
)

var (
	benchmarkHyperLogLogCountSink uint64
	benchmarkHyperLogLogInfoSink  HyperLogLogInfo
	benchmarkHyperLogLogDataSink  []hyperLogLogData
)

func TestHyperLogLogEstimateMatchesFullRegisterScan(t *testing.T) {
	tests := []struct {
		precision uint8
		updates   int
	}{
		{precision: 4, updates: 512},
		{precision: 10, updates: 4096},
		{precision: 14, updates: 512},
		{precision: 20, updates: 64},
	}

	for _, tc := range tests {
		t.Run("precision_"+strconv.Itoa(int(tc.precision)), func(t *testing.T) {
			hll, err := newHyperLogLogData(tc.precision)
			if err != nil {
				t.Fatalf("newHyperLogLogData(%d) error = %v", tc.precision, err)
			}
			assertHyperLogLogMatchesFullScan(t, hll)

			for update := 0; update < tc.updates; update++ {
				value := "hll-estimate:" + strconv.FormatUint(uint64(update)*2654435761, 10)
				hll.addJSONString(value)
				assertHyperLogLogMatchesFullScan(t, hll)
			}

			before := hll.Count()
			for duplicate := 0; duplicate < 32; duplicate++ {
				hll.addJSONString("hll-estimate:0")
				assertHyperLogLogMatchesFullScan(t, hll)
			}
			if got := hll.Count(); got != before {
				t.Fatalf("Count() after duplicates = %d, want %d", got, before)
			}
		})
	}
}

func TestHyperLogLogEstimateMatchesFullScanAfterSnapshotRestore(t *testing.T) {
	hll, err := newHyperLogLogData(14)
	if err != nil {
		t.Fatalf("newHyperLogLogData() error = %v", err)
	}
	for update := 0; update < 4096; update++ {
		hll.addJSONString("snapshot-estimate:" + strconv.Itoa(update))
	}

	restored, err := newHyperLogLogDataFromSnapshot(hll.Snapshot())
	if err != nil {
		t.Fatalf("newHyperLogLogDataFromSnapshot() error = %v", err)
	}
	assertHyperLogLogMatchesFullScan(t, restored)
	if got, want := restored.Count(), hll.Count(); got != want {
		t.Fatalf("restored Count() = %d, want %d", got, want)
	}

	for update := 4096; update < 8192; update++ {
		restored.addJSONString("snapshot-estimate:" + strconv.Itoa(update))
		assertHyperLogLogMatchesFullScan(t, restored)
	}
}

func TestHyperLogLogIncrementalEstimateMatchesLongDistinctStream(t *testing.T) {
	tests := []struct {
		precision uint8
		updates   int
		checkEach int
	}{
		{precision: 10, updates: 200000, checkEach: 257},
		{precision: 14, updates: 200000, checkEach: 257},
		{precision: 20, updates: 20000, checkEach: 1024},
	}

	for _, tc := range tests {
		t.Run("precision_"+strconv.Itoa(int(tc.precision)), func(t *testing.T) {
			hll, err := newHyperLogLogData(tc.precision)
			if err != nil {
				t.Fatalf("newHyperLogLogData(%d) error = %v", tc.precision, err)
			}
			for update := 0; update < tc.updates; update++ {
				hll.addJSONString("long-stream:" + strconv.Itoa(update))
				if update%tc.checkEach == 0 {
					assertHyperLogLogMatchesFullScan(t, hll)
				}
			}
			assertHyperLogLogMatchesFullScan(t, hll)
		})
	}
}

func TestHyperLogLogSummaryLayoutAndSnapshotIsolation(t *testing.T) {
	if got := unsafe.Sizeof(hyperLogLogData{}); got != 48 {
		t.Fatalf("sizeof(hyperLogLogData) = %d, want 48", got)
	}
	hll, err := newHyperLogLogData(DefaultHyperLogLogPrecision)
	if err != nil {
		t.Fatalf("newHyperLogLogData() error = %v", err)
	}
	if hll.registers != nil {
		t.Fatalf("empty registers = %#v, want nil", hll.registers)
	}
	hll.addJSONString("value")
	if got, want := len(hll.registers), hyperLogLogRegisterCount(DefaultHyperLogLogPrecision); got != want {
		t.Fatalf("register length = %d, want %d", got, want)
	}
	if got, want := cap(hll.registers), len(hll.registers); got != want {
		t.Fatalf("register capacity = %d, want exact %d", got, want)
	}
	snapshot := hll.Snapshot()
	raw, err := base64.StdEncoding.DecodeString(snapshot.Registers)
	if err != nil {
		t.Fatalf("DecodeString(snapshot registers) error = %v", err)
	}
	if got, want := len(raw), len(hll.registers); got != want {
		t.Fatalf("snapshot register bytes = %d, want %d without derived summary", got, want)
	}
	if got, want := hll.EncodedSize(), int64(len(hll.registers)); got != want {
		t.Fatalf("EncodedSize() = %d, want %d", got, want)
	}
}

func TestHyperLogLogFullScanReferenceCoversAdversarialRanks(t *testing.T) {
	precision := uint8(10)
	registers := make([]uint8, hyperLogLogRegisterCount(precision))
	maxRank := hyperLogLogMaxRank(precision)
	for idx := range registers {
		switch idx % 4 {
		case 0:
			registers[idx] = 0
		case 1:
			registers[idx] = 1
		case 2:
			registers[idx] = maxRank / 2
		case 3:
			registers[idx] = maxRank
		}
	}
	hll := hyperLogLogData{
		registers: registers,
		precision: precision,
	}
	assertHyperLogLogMatchesFullScan(t, hll)
	hll.addJSONString("legacy-header-upgrade")
	if !hll.summaryReady() {
		t.Fatal("mutation did not rebuild missing derived summary")
	}
	assertHyperLogLogMatchesFullScan(t, hll)
}

func assertHyperLogLogMatchesFullScan(t *testing.T, hll hyperLogLogData) {
	t.Helper()
	wantCount, wantNonZero := fullScanHyperLogLogState(hll)
	if got := hll.Count(); got != wantCount {
		t.Fatalf("Count() = %d, full register scan = %d", got, wantCount)
	}
	info := hll.Info()
	if info.Estimate != wantCount {
		t.Fatalf("Info().Estimate = %d, full register scan = %d", info.Estimate, wantCount)
	}
	if info.NonZeroRegisters != wantNonZero {
		t.Fatalf("Info().NonZeroRegisters = %d, full register scan = %d", info.NonZeroRegisters, wantNonZero)
	}
}

func fullScanHyperLogLogState(hll hyperLogLogData) (uint64, uint64) {
	m := float64(len(hll.registers))
	if m == 0 {
		return 0, 0
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
		return 0, uint64(len(hll.registers) - zeros)
	}
	raw := hyperLogLogAlpha(m) * m * m / sum
	if raw <= 2.5*m && zeros > 0 {
		raw = m * math.Log(m/float64(zeros))
	} else {
		const two64 = 18446744073709551616.0
		if raw > two64/30 {
			corrected := -two64 * math.Log1p(-raw/two64)
			if !math.IsInf(corrected, 0) && !math.IsNaN(corrected) && corrected > 0 {
				raw = corrected
			}
		}
	}
	return hyperLogLogEstimateUint64(raw), uint64(len(hll.registers) - zeros)
}

func BenchmarkHyperLogLogEstimateOperations(b *testing.B) {
	for _, precision := range []uint8{10, 14} {
		name := "Precision" + strconv.Itoa(int(precision))
		hll, err := newHyperLogLogData(precision)
		if err != nil {
			b.Fatal(err)
		}
		hll.addJSONString("value")

		b.Run(name+"/Count", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				benchmarkHyperLogLogCountSink = hll.Count()
			}
		})
		b.Run(name+"/Info", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				benchmarkHyperLogLogInfoSink = hll.Info()
			}
		})
		b.Run(name+"/DuplicateAddAndCount", func(b *testing.B) {
			candidate := hll
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				candidate.addJSONString("value")
				benchmarkHyperLogLogCountSink = candidate.Count()
			}
		})
	}
}

func BenchmarkHyperLogLogLayout1000(b *testing.B) {
	const filters = 1000
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		data := make([]hyperLogLogData, filters)
		var backingBytes int
		for idx := range data {
			data[idx], _ = newHyperLogLogData(DefaultHyperLogLogPrecision)
			data[idx].addJSONString("value")
			backingBytes += cap(data[idx].registers)
		}
		benchmarkHyperLogLogDataSink = data
		b.ReportMetric(float64(backingBytes)/filters, "register_cap_B/filter")
	}
	b.ReportMetric(float64(unsafe.Sizeof(hyperLogLogData{})), "struct_B/filter")
}
