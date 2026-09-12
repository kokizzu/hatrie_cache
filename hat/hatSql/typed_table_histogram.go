package hatSql

import (
	"fmt"
	"math"
	"math/bits"
)

const (
	// DefaultTypedTableHistogramBins is used when Histogram receives zero bins.
	DefaultTypedTableHistogramBins = 32
	// MaxTypedTableHistogramBins bounds retained histogram state.
	MaxTypedTableHistogramBins = 256
)

// TypedTableHistogramOptions controls one on-demand histogram snapshot. A
// zero bin count uses DefaultTypedTableHistogramBins.
type TypedTableHistogramOptions struct {
	Bins int
}

// TypedTableHistogramBin stores the inclusive range and number of finite
// values in one histogram bucket. All bins except the last are lower-inclusive
// and upper-exclusive for floating-point values; integer bounds are exact.
type TypedTableHistogramBin struct {
	Lower TypedTableValue
	Upper TypedTableValue
	Count int
}

// TypedTableHistogram is a bounded numeric distribution snapshot. NullCount
// and UnbucketedCount are excluded from Bins; UnbucketedCount contains valid
// NaN or infinite float values.
type TypedTableHistogram struct {
	Field           string
	Kind            TypedTableKind
	RowCount        int
	NullCount       int
	ValueCount      int
	UnbucketedCount int
	HasMinMax       bool
	Min             TypedTableValue
	Max             TypedTableValue
	Bins            []TypedTableHistogramBin
}

// Histogram computes a compact, exact-count histogram for an int64 or float64
// column over active rows. It scans existing typed storage under the table
// read lock and retains only the bounded result bins.
func (table *TypedTable) Histogram(field string, options TypedTableHistogramOptions) (TypedTableHistogram, error) {
	if table == nil {
		return TypedTableHistogram{}, fmt.Errorf("typed table is nil")
	}
	bins, err := typedTableHistogramBinLimit(options.Bins)
	if err != nil {
		return TypedTableHistogram{}, err
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	columnIndex, found := table.byName[field]
	if !found {
		return TypedTableHistogram{}, fmt.Errorf("typed table histogram field %q is missing", field)
	}
	storage := &table.columns[columnIndex]
	if storage.kind != TypedTableInt64 && storage.kind != TypedTableFloat64 {
		return TypedTableHistogram{}, fmt.Errorf("typed table histogram field %q has unsupported kind %d", field, storage.kind)
	}
	histogram := TypedTableHistogram{Field: field, Kind: storage.kind}
	var minInt, maxInt int64
	var minFloat, maxFloat float64
	for row := range table.keys {
		if table.typedTableRowDeletedLocked(row) {
			continue
		}
		histogram.RowCount++
		if !storage.valid[row] {
			histogram.NullCount++
			continue
		}
		histogram.ValueCount++
		if storage.kind == TypedTableInt64 {
			value := storage.int64s[row]
			if !histogram.HasMinMax {
				minInt, maxInt = value, value
				histogram.Min = TypedInt64(value)
				histogram.Max = TypedInt64(value)
				histogram.HasMinMax = true
			} else {
				if value < minInt {
					minInt = value
					histogram.Min = TypedInt64(value)
				}
				if value > maxInt {
					maxInt = value
					histogram.Max = TypedInt64(value)
				}
			}
			continue
		}
		value := storage.floats[row]
		if math.IsNaN(value) || math.IsInf(value, 0) {
			histogram.UnbucketedCount++
			continue
		}
		if !histogram.HasMinMax {
			minFloat, maxFloat = value, value
			histogram.Min = TypedFloat64(value)
			histogram.Max = TypedFloat64(value)
			histogram.HasMinMax = true
		} else {
			if value < minFloat {
				minFloat = value
				histogram.Min = TypedFloat64(value)
			}
			if value > maxFloat {
				maxFloat = value
				histogram.Max = TypedFloat64(value)
			}
		}
	}
	if !histogram.HasMinMax {
		return histogram, nil
	}
	if storage.kind == TypedTableInt64 {
		bins = typedTableIntHistogramBinCount(minInt, maxInt, bins)
		histogram.Bins = make([]TypedTableHistogramBin, bins)
		for index := range histogram.Bins {
			lower, upper := typedTableIntHistogramBounds(minInt, maxInt, bins, index)
			histogram.Bins[index] = TypedTableHistogramBin{Lower: TypedInt64(lower), Upper: TypedInt64(upper)}
		}
		for row := range table.keys {
			if table.typedTableRowDeletedLocked(row) || !storage.valid[row] {
				continue
			}
			index := typedTableIntHistogramIndex(storage.int64s[row], minInt, maxInt, bins)
			histogram.Bins[index].Count++
		}
		return histogram, nil
	}
	if span := maxFloat - minFloat; span == 0 || math.IsInf(span, 0) {
		bins = 1
	}
	histogram.Bins = make([]TypedTableHistogramBin, bins)
	for index := range histogram.Bins {
		lower, upper := typedTableFloatHistogramBounds(minFloat, maxFloat, bins, index)
		histogram.Bins[index] = TypedTableHistogramBin{Lower: TypedFloat64(lower), Upper: TypedFloat64(upper)}
	}
	for row := range table.keys {
		if table.typedTableRowDeletedLocked(row) || !storage.valid[row] {
			continue
		}
		value := storage.floats[row]
		if math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		index := typedTableFloatHistogramIndex(value, minFloat, maxFloat, bins)
		histogram.Bins[index].Count++
	}
	return histogram, nil
}

func typedTableHistogramBinLimit(requested int) (int, error) {
	if requested == 0 {
		return DefaultTypedTableHistogramBins, nil
	}
	if requested < 0 || requested > MaxTypedTableHistogramBins {
		return 0, fmt.Errorf("typed table histogram bins must be between 1 and %d", MaxTypedTableHistogramBins)
	}
	return requested, nil
}

func typedTableIntHistogramBinCount(minimum, maximum int64, requested int) int {
	minOrder := typedTableOrderedInt(minimum)
	maxOrder := typedTableOrderedInt(maximum)
	span := maxOrder - minOrder
	if span != math.MaxUint64 && span+1 < uint64(requested) {
		return int(span + 1)
	}
	return requested
}

func typedTableIntHistogramBounds(minimum, maximum int64, bins, index int) (int64, int64) {
	minOrder := typedTableOrderedInt(minimum)
	maxOrder := typedTableOrderedInt(maximum)
	span := maxOrder - minOrder
	total := span + 1
	lowerOffset := typedTableHistogramPartition(total, index, bins)
	var upperOrder uint64
	if index+1 == bins {
		upperOrder = maxOrder
	} else {
		upperOffset := typedTableHistogramPartition(total, index+1, bins)
		upperOrder = minOrder + upperOffset - 1
	}
	lowerOrder := minOrder + lowerOffset
	return typedTableUnorderedInt(lowerOrder), typedTableUnorderedInt(upperOrder)
}

func typedTableIntHistogramIndex(value, minimum, maximum int64, bins int) int {
	minOrder := typedTableOrderedInt(minimum)
	valueOffset := typedTableOrderedInt(value) - minOrder
	span := typedTableOrderedInt(maximum) - minOrder
	hi, lo := bits.Mul64(valueOffset, uint64(bins))
	var quotient uint64
	if span == math.MaxUint64 {
		quotient = hi
	} else {
		quotient, _ = bits.Div64(hi, lo, span+1)
	}
	if quotient >= uint64(bins) {
		return bins - 1
	}
	return int(quotient)
}

func typedTableHistogramPartition(total uint64, index, bins int) uint64 {
	if total == 0 {
		quotient, _ := bits.Div64(uint64(index), 0, uint64(bins))
		return quotient
	}
	hi, lo := bits.Mul64(total, uint64(index))
	quotient, _ := bits.Div64(hi, lo, uint64(bins))
	return quotient
}

func typedTableOrderedInt(value int64) uint64 {
	return uint64(value) ^ (uint64(1) << 63)
}

func typedTableUnorderedInt(value uint64) int64 {
	return int64(value ^ (uint64(1) << 63))
}

func typedTableFloatHistogramBounds(minimum, maximum float64, bins, index int) (float64, float64) {
	if bins == 1 || minimum == maximum {
		return minimum, maximum
	}
	span := maximum - minimum
	lower := minimum + span*float64(index)/float64(bins)
	upper := maximum
	if index+1 != bins {
		upper = minimum + span*float64(index+1)/float64(bins)
	}
	return lower, upper
}

func typedTableFloatHistogramIndex(value, minimum, maximum float64, bins int) int {
	if bins == 1 || minimum == maximum {
		return 0
	}
	index := int((value - minimum) / (maximum - minimum) * float64(bins))
	if index < 0 {
		return 0
	}
	if index >= bins {
		return bins - 1
	}
	return index
}
