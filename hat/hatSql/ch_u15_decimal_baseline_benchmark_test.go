package hatSql

import (
	"strconv"
	"testing"
)

const benchmarkSQLDecimalKernelRows = 1024

var (
	benchmarkSQLDecimalKernelIntSink    int
	benchmarkSQLDecimalKernelBitmapSink uint64
	benchmarkSQLDecimalKernel128Sink    SQLDecimal128
	benchmarkSQLDecimalKernel256Sink    SQLDecimal256
)

func benchmarkSQLDecimal128KernelValues(b *testing.B) []SQLDecimal128 {
	b.Helper()
	values := make([]SQLDecimal128, benchmarkSQLDecimalKernelRows)
	for index := range values {
		value, err := ParseSQLDecimal128(strconv.Itoa(index-(benchmarkSQLDecimalKernelRows/2)), 0)
		if err != nil {
			b.Fatal(err)
		}
		values[index] = value
	}
	return values
}

func benchmarkSQLDecimal256KernelValues(b *testing.B) []SQLDecimal256 {
	b.Helper()
	values := make([]SQLDecimal256, benchmarkSQLDecimalKernelRows)
	for index := range values {
		value, err := ParseSQLDecimal256(strconv.Itoa(index-(benchmarkSQLDecimalKernelRows/2)), 0)
		if err != nil {
			b.Fatal(err)
		}
		values[index] = value
	}
	return values
}

func benchmarkSQLDecimalBytewiseCompare(left, right []byte) int {
	leftNegative := len(left) != 0 && left[len(left)-1]&0x80 != 0
	rightNegative := len(right) != 0 && right[len(right)-1]&0x80 != 0
	if leftNegative != rightNegative {
		if leftNegative {
			return -1
		}
		return 1
	}
	for index := len(left) - 1; index >= 0; index-- {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}

func benchmarkSQLDecimalBytewiseAdd(destination, left, right []byte) {
	var carry uint16
	for index := range destination {
		sum := uint16(left[index]) + uint16(right[index]) + carry
		destination[index] = byte(sum)
		carry = sum >> 8
	}
}

func benchmarkSQLDecimalBytewiseFilter128(values []SQLDecimal128, target SQLDecimal128, destination []uint64) int {
	count := 0
	for wordIndex := range destination {
		start := wordIndex * 64
		end := start + 64
		if end > len(values) {
			end = len(values)
		}
		var matches uint64
		for index := start; index < end; index++ {
			if benchmarkSQLDecimalBytewiseCompare(values[index][:], target[:]) >= 0 {
				matches |= uint64(1) << uint(index-start)
			}
		}
		destination[wordIndex] = matches
		count += benchmarkSQLDecimalBitCount(matches)
	}
	return count
}

func benchmarkSQLDecimalBytewiseFilter256(values []SQLDecimal256, target SQLDecimal256, destination []uint64) int {
	count := 0
	for wordIndex := range destination {
		start := wordIndex * 64
		end := start + 64
		if end > len(values) {
			end = len(values)
		}
		var matches uint64
		for index := start; index < end; index++ {
			if benchmarkSQLDecimalBytewiseCompare(values[index][:], target[:]) >= 0 {
				matches |= uint64(1) << uint(index-start)
			}
		}
		destination[wordIndex] = matches
		count += benchmarkSQLDecimalBitCount(matches)
	}
	return count
}

func benchmarkSQLDecimalBitCount(value uint64) int {
	count := 0
	for value != 0 {
		value &= value - 1
		count++
	}
	return count
}

func BenchmarkSQLDecimalKernelBaseline(b *testing.B) {
	b.Run("compare128", func(b *testing.B) {
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
	b.Run("compare256", func(b *testing.B) {
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
	b.Run("add128", func(b *testing.B) {
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
	b.Run("add256", func(b *testing.B) {
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
	b.Run("filter128", func(b *testing.B) {
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
	b.Run("filter256", func(b *testing.B) {
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
}
