package hatDataStructure

import "testing"

var defaultValueColumnBenchmarkSink any

func defaultValueColumnSparseValues(rows int) []int64 {
	values := make([]int64, rows)
	for index := range values {
		if index%64 == 0 {
			values[index] = int64(index + 1)
		}
	}
	return values
}

func BenchmarkDefaultValueColumnAppendSparse(b *testing.B) {
	const rows = 4096
	values := defaultValueColumnSparseValues(rows)
	b.Run("DenseBaseline", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			column := make([]int64, 0, rows)
			for _, value := range values {
				column = append(column, value)
			}
			defaultValueColumnBenchmarkSink = column
		}
	})
	b.Run("AdaptiveSparse", func(b *testing.B) {
		b.ReportAllocs()
		var last *DefaultValueColumn[int64]
		for iteration := 0; iteration < b.N; iteration++ {
			column, err := NewDefaultValueColumn[int64](0, rows)
			if err != nil {
				b.Fatal(err)
			}
			for _, value := range values {
				if err := column.Append(value); err != nil {
					b.Fatal(err)
				}
			}
			last = column
		}
		b.ReportMetric(float64(last.StorageBytes()), "retained-bytes")
		defaultValueColumnBenchmarkSink = last
	})
}

func BenchmarkDefaultValueColumnLookupSparse(b *testing.B) {
	const rows = 4096
	values := defaultValueColumnSparseValues(rows)
	dense := make([]int64, rows)
	column, err := NewDefaultValueColumn[int64](0, rows)
	if err != nil {
		b.Fatal(err)
	}
	for index, value := range values {
		dense[index] = value
		if err := column.Append(value); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("DenseBaseline", func(b *testing.B) {
		b.ReportAllocs()
		var sink int64
		for iteration := 0; iteration < b.N; iteration++ {
			sink += dense[(iteration*31)%rows]
		}
		defaultValueColumnBenchmarkSink = sink
	})
	b.Run("AdaptiveSparse", func(b *testing.B) {
		b.ReportAllocs()
		var sink int64
		for iteration := 0; iteration < b.N; iteration++ {
			value, ok := column.Get((iteration * 31) % rows)
			if !ok {
				b.Fatal("unexpected missing row")
			}
			sink += value
		}
		b.ReportMetric(float64(column.StorageBytes()), "retained-bytes")
		defaultValueColumnBenchmarkSink = sink
	})
}

func BenchmarkDefaultValueColumnDensity(b *testing.B) {
	const rows = 4096
	for _, density := range []struct {
		name  string
		count int
	}{
		{name: "1pct", count: rows / 100},
		{name: "25pct", count: rows / 4},
		{name: "50pct", count: rows / 2},
		{name: "75pct", count: rows * 3 / 4},
		{name: "100pct", count: rows},
	} {
		b.Run(density.name, func(b *testing.B) {
			values := make([]int64, rows)
			for index := range values {
				isNonDefault := (index+1)*density.count/rows > index*density.count/rows
				if isNonDefault {
					values[index] = int64(index + 1)
				}
			}
			b.ReportAllocs()
			var last *DefaultValueColumn[int64]
			for iteration := 0; iteration < b.N; iteration++ {
				column, err := NewDefaultValueColumn[int64](0, rows)
				if err != nil {
					b.Fatal(err)
				}
				for _, value := range values {
					if err := column.Append(value); err != nil {
						b.Fatal(err)
					}
				}
				last = column
			}
			b.ReportMetric(float64(last.StorageBytes()), "retained-bytes")
			b.ReportMetric(float64(last.NonDefaultCount()), "nondefaults")
			defaultValueColumnBenchmarkSink = last
		})
	}
}
