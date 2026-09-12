package hatSql

import (
	"math"
	"strconv"
	"testing"
)

func TestTypedTableHistogramBuildsCompactExactIntegerBins(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "score", Kind: TypedTableInt64},
			{Name: "ratio", Kind: TypedTableFloat64},
			{Name: "label", Kind: TypedTableString},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := int64(0); index < 100; index++ {
		if _, err := table.Upsert(strconv.FormatInt(index, 10), []TypedTableValue{
			TypedInt64(index), TypedFloat64(float64(index) / 10), TypedString("event"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.Upsert("null", []TypedTableValue{TypedNull(), TypedFloat64(math.NaN()), TypedString("null")}); err != nil {
		t.Fatal(err)
	}

	histogram, err := table.Histogram("score", TypedTableHistogramOptions{Bins: 4})
	if err != nil {
		t.Fatal(err)
	}
	if histogram.Field != "score" || histogram.Kind != TypedTableInt64 || histogram.RowCount != 101 || histogram.NullCount != 1 || histogram.ValueCount != 100 || histogram.UnbucketedCount != 0 {
		t.Fatalf("histogram header = %#v, want field/rows/nulls/values/unbucketed score/101/1/100/0", histogram)
	}
	if !histogram.HasMinMax || histogram.Min != TypedInt64(0) || histogram.Max != TypedInt64(99) {
		t.Fatalf("histogram min/max = %#v/%#v, want 0/99", histogram.Min, histogram.Max)
	}
	if len(histogram.Bins) != 4 {
		t.Fatalf("len(histogram.Bins) = %d, want 4", len(histogram.Bins))
	}
	for index, bin := range histogram.Bins {
		wantLower := int64(index * 25)
		wantUpper := wantLower + 24
		if bin.Count != 25 || bin.Lower != TypedInt64(wantLower) || bin.Upper != TypedInt64(wantUpper) {
			t.Fatalf("bin %d = %#v, want count=25 bounds=%d..%d", index, bin, wantLower, wantUpper)
		}
	}

	floatHistogram, err := table.Histogram("ratio", TypedTableHistogramOptions{Bins: 5})
	if err != nil {
		t.Fatal(err)
	}
	if floatHistogram.ValueCount != 101 || floatHistogram.UnbucketedCount != 1 || floatHistogram.NullCount != 0 {
		t.Fatalf("float histogram counts = %#v, want values=101 unbucketed=1 nulls=0", floatHistogram)
	}
	if got := histogramBinCount(floatHistogram); got != 100 {
		t.Fatalf("float binned count = %d, want 100", got)
	}

	if _, err := table.Delete("0"); err != nil {
		t.Fatal(err)
	}
	histogram, err = table.Histogram("score", TypedTableHistogramOptions{Bins: 4})
	if err != nil {
		t.Fatal(err)
	}
	if histogram.RowCount != 100 || histogram.ValueCount != 99 || histogram.NullCount != 1 || histogram.Min != TypedInt64(1) || histogram.Max != TypedInt64(99) {
		t.Fatalf("histogram after delete = %#v, want rows=100 values=99 nulls=1 min=1 max=99", histogram)
	}
}

func TestTypedTableHistogramDefaultsAndRejectsInvalidInputs(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "score", Kind: TypedTableInt64}, {Name: "label", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("one", []TypedTableValue{TypedInt64(7), TypedString("one")}); err != nil {
		t.Fatal(err)
	}
	histogram, err := table.Histogram("score", TypedTableHistogramOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(histogram.Bins) != 1 || histogram.Bins[0].Count != 1 {
		t.Fatalf("default histogram = %#v, want one populated compact bin", histogram)
	}
	for _, test := range []struct {
		name  string
		field string
		bins  int
	}{
		{name: "missing field", field: "missing"},
		{name: "string field", field: "label"},
		{name: "negative bins", field: "score", bins: -1},
		{name: "too many bins", field: "score", bins: MaxTypedTableHistogramBins + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := table.Histogram(test.field, TypedTableHistogramOptions{Bins: test.bins}); err == nil {
				t.Fatal("Histogram() error = nil, want validation error")
			}
		})
	}
	if _, err := (*TypedTable)(nil).Histogram("score", TypedTableHistogramOptions{}); err == nil {
		t.Fatal("nil Histogram() error = nil, want error")
	}
}

func TestTypedTableHistogramHandlesFullInt64Range(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{Name: "events", Columns: []TypedTableColumn{{Name: "score", Kind: TypedTableInt64}}})
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]int64{"min": math.MinInt64, "zero": 0, "max": math.MaxInt64} {
		if _, err := table.Upsert(key, []TypedTableValue{TypedInt64(value)}); err != nil {
			t.Fatal(err)
		}
	}
	histogram, err := table.Histogram("score", TypedTableHistogramOptions{Bins: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(histogram.Bins) != 2 {
		t.Fatalf("len(histogram.Bins) = %d, want 2", len(histogram.Bins))
	}
	if histogram.Bins[0].Lower != TypedInt64(math.MinInt64) || histogram.Bins[0].Upper != TypedInt64(-1) || histogram.Bins[0].Count != 1 {
		t.Fatalf("lower full-range bin = %#v, want MinInt64..-1 count 1", histogram.Bins[0])
	}
	if histogram.Bins[1].Lower != TypedInt64(0) || histogram.Bins[1].Upper != TypedInt64(math.MaxInt64) || histogram.Bins[1].Count != 2 {
		t.Fatalf("upper full-range bin = %#v, want 0..MaxInt64 count 2", histogram.Bins[1])
	}
}

func TestTypedTableHistogramHandlesExtremeAndOnlySpecialFloats(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{Name: "events", Columns: []TypedTableColumn{{Name: "score", Kind: TypedTableFloat64}}})
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]float64{"low": -math.MaxFloat64, "high": math.MaxFloat64} {
		if _, err := table.Upsert(key, []TypedTableValue{TypedFloat64(value)}); err != nil {
			t.Fatal(err)
		}
	}
	histogram, err := table.Histogram("score", TypedTableHistogramOptions{Bins: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(histogram.Bins) != 1 || histogram.Bins[0].Count != 2 {
		t.Fatalf("extreme float histogram = %#v, want one bin containing both values", histogram)
	}

	special, err := NewTypedTable(TypedTableSchema{Name: "special", Columns: []TypedTableColumn{{Name: "score", Kind: TypedTableFloat64}}})
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]float64{"nan": math.NaN(), "positive": math.Inf(1), "negative": math.Inf(-1)} {
		if _, err := special.Upsert(key, []TypedTableValue{TypedFloat64(value)}); err != nil {
			t.Fatal(err)
		}
	}
	histogram, err = special.Histogram("score", TypedTableHistogramOptions{Bins: 4})
	if err != nil {
		t.Fatal(err)
	}
	if histogram.HasMinMax || histogram.ValueCount != 3 || histogram.UnbucketedCount != 3 || len(histogram.Bins) != 0 {
		t.Fatalf("only-special float histogram = %#v, want no bins and three unbucketed values", histogram)
	}

	flat, err := NewTypedTable(TypedTableSchema{Name: "flat", Columns: []TypedTableColumn{{Name: "score", Kind: TypedTableFloat64}}})
	if err != nil {
		t.Fatal(err)
	}
	for key := range 3 {
		if _, err := flat.Upsert("key-"+string(rune('a'+key)), []TypedTableValue{TypedFloat64(2)}); err != nil {
			t.Fatal(err)
		}
	}
	histogram, err = flat.Histogram("score", TypedTableHistogramOptions{Bins: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(histogram.Bins) != 1 || histogram.Bins[0].Count != 3 {
		t.Fatalf("flat float histogram = %#v, want one compact bin containing three values", histogram)
	}
}

func histogramBinCount(histogram TypedTableHistogram) int {
	count := 0
	for _, bin := range histogram.Bins {
		count += bin.Count
	}
	return count
}
