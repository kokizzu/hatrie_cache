package hatSql

import "testing"

func BenchmarkSQLDecimalKernel(b *testing.B) {
	b.Run("compare128/bytewise", func(b *testing.B) {
		values := benchmarkSQLDecimal128KernelValues(b)
		target := values[len(values)/2]
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			total := 0
			for index := range values {
				total += benchmarkSQLDecimalBytewiseCompare(values[index][:], target[:])
			}
			benchmarkSQLDecimalKernelIntSink = total
		}
	})
	b.Run("compare128/word", func(b *testing.B) {
		values := benchmarkSQLDecimal128KernelValues(b)
		target := values[len(values)/2]
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			total := 0
			for index := range values {
				total += values[index].Compare(target)
			}
			benchmarkSQLDecimalKernelIntSink = total
		}
	})
	b.Run("compare256/bytewise", func(b *testing.B) {
		values := benchmarkSQLDecimal256KernelValues(b)
		target := values[len(values)/2]
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			total := 0
			for index := range values {
				total += benchmarkSQLDecimalBytewiseCompare(values[index][:], target[:])
			}
			benchmarkSQLDecimalKernelIntSink = total
		}
	})
	b.Run("compare256/word", func(b *testing.B) {
		values := benchmarkSQLDecimal256KernelValues(b)
		target := values[len(values)/2]
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			total := 0
			for index := range values {
				total += values[index].Compare(target)
			}
			benchmarkSQLDecimalKernelIntSink = total
		}
	})
	b.Run("add128/bytewise", func(b *testing.B) {
		values := benchmarkSQLDecimal128KernelValues(b)
		var target SQLDecimal128
		var result SQLDecimal128
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for index := range values {
				benchmarkSQLDecimalBytewiseAdd(result[:], values[index][:], target[:])
			}
			benchmarkSQLDecimalKernel128Sink = result
		}
	})
	b.Run("add128/word", func(b *testing.B) {
		values := benchmarkSQLDecimal128KernelValues(b)
		var target SQLDecimal128
		var result SQLDecimal128
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for index := range values {
				result, _ = AddSQLDecimal128(values[index], target)
			}
			benchmarkSQLDecimalKernel128Sink = result
		}
	})
	b.Run("add256/bytewise", func(b *testing.B) {
		values := benchmarkSQLDecimal256KernelValues(b)
		var target SQLDecimal256
		var result SQLDecimal256
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for index := range values {
				benchmarkSQLDecimalBytewiseAdd(result[:], values[index][:], target[:])
			}
			benchmarkSQLDecimalKernel256Sink = result
		}
	})
	b.Run("add256/word", func(b *testing.B) {
		values := benchmarkSQLDecimal256KernelValues(b)
		var target SQLDecimal256
		var result SQLDecimal256
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for index := range values {
				result, _ = AddSQLDecimal256(values[index], target)
			}
			benchmarkSQLDecimalKernel256Sink = result
		}
	})
	b.Run("filter128/bytewise", func(b *testing.B) {
		values := benchmarkSQLDecimal128KernelValues(b)
		target := values[len(values)/2]
		bitmap := make([]uint64, (len(values)+63)/64)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkSQLDecimalKernelIntSink = benchmarkSQLDecimalBytewiseFilter128(values, target, bitmap)
			benchmarkSQLDecimalKernelBitmapSink = bitmap[0]
		}
	})
	b.Run("filter128/word", func(b *testing.B) {
		values := benchmarkSQLDecimal128KernelValues(b)
		target := values[len(values)/2]
		bitmap := make([]uint64, (len(values)+63)/64)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			count, err := FilterSQLDecimal128Batch(values, target, SQLDecimalGreaterOrEqual, bitmap)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSQLDecimalKernelIntSink = count
			benchmarkSQLDecimalKernelBitmapSink = bitmap[0]
		}
	})
	b.Run("filter256/bytewise", func(b *testing.B) {
		values := benchmarkSQLDecimal256KernelValues(b)
		target := values[len(values)/2]
		bitmap := make([]uint64, (len(values)+63)/64)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkSQLDecimalKernelIntSink = benchmarkSQLDecimalBytewiseFilter256(values, target, bitmap)
			benchmarkSQLDecimalKernelBitmapSink = bitmap[0]
		}
	})
	b.Run("filter256/word", func(b *testing.B) {
		values := benchmarkSQLDecimal256KernelValues(b)
		target := values[len(values)/2]
		bitmap := make([]uint64, (len(values)+63)/64)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			count, err := FilterSQLDecimal256Batch(values, target, SQLDecimalGreaterOrEqual, bitmap)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkSQLDecimalKernelIntSink = count
			benchmarkSQLDecimalKernelBitmapSink = bitmap[0]
		}
	})
}
