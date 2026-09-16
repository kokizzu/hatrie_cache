package hatSql

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// SQLDecimalComparison selects the predicate used by a decimal batch filter.
type SQLDecimalComparison uint8

const (
	SQLDecimalEqual SQLDecimalComparison = iota
	SQLDecimalNotEqual
	SQLDecimalLess
	SQLDecimalLessOrEqual
	SQLDecimalGreater
	SQLDecimalGreaterOrEqual
)

func (comparison SQLDecimalComparison) valid() bool {
	return comparison <= SQLDecimalGreaterOrEqual
}

func (comparison SQLDecimalComparison) matches(order int) bool {
	switch comparison {
	case SQLDecimalEqual:
		return order == 0
	case SQLDecimalNotEqual:
		return order != 0
	case SQLDecimalLess:
		return order < 0
	case SQLDecimalLessOrEqual:
		return order <= 0
	case SQLDecimalGreater:
		return order > 0
	case SQLDecimalGreaterOrEqual:
		return order >= 0
	default:
		return false
	}
}

// Compare returns the signed ordering of two Decimal128 coefficients.
func (value SQLDecimal128) Compare(other SQLDecimal128) int {
	return compareSQLDecimalFixedWords(value[:], other[:])
}

// Compare returns the signed ordering of two Decimal256 coefficients.
func (value SQLDecimal256) Compare(other SQLDecimal256) int {
	return compareSQLDecimalFixedWords(value[:], other[:])
}

// AddSQLDecimal128 adds two same-scale coefficients. The result wraps at
// 128 bits; overflow reports whether the signed result exceeded that width.
func AddSQLDecimal128(left, right SQLDecimal128) (result SQLDecimal128, overflow bool) {
	overflow = addSQLDecimalFixed(result[:], left[:], right[:])
	return result, overflow
}

// AddSQLDecimal256 adds two same-scale coefficients. The result wraps at
// 256 bits; overflow reports whether the signed result exceeded that width.
func AddSQLDecimal256(left, right SQLDecimal256) (result SQLDecimal256, overflow bool) {
	overflow = addSQLDecimalFixed(result[:], left[:], right[:])
	return result, overflow
}

// SubSQLDecimal128 subtracts two same-scale coefficients. The result wraps at
// 128 bits; overflow reports whether the signed result exceeded that width.
func SubSQLDecimal128(left, right SQLDecimal128) (result SQLDecimal128, overflow bool) {
	overflow = subSQLDecimalFixed(result[:], left[:], right[:])
	return result, overflow
}

// SubSQLDecimal256 subtracts two same-scale coefficients. The result wraps at
// 256 bits; overflow reports whether the signed result exceeded that width.
func SubSQLDecimal256(left, right SQLDecimal256) (result SQLDecimal256, overflow bool) {
	overflow = subSQLDecimalFixed(result[:], left[:], right[:])
	return result, overflow
}

// FilterSQLDecimal128Batch compares values to target and packs matching row
// positions into destination. The caller owns destination and must provide at
// least (len(values)+63)/64 words. It performs no allocation on valid input.
func FilterSQLDecimal128Batch(values []SQLDecimal128, target SQLDecimal128, comparison SQLDecimalComparison, destination []uint64) (int, error) {
	if !comparison.valid() {
		return 0, fmt.Errorf("hatSql: invalid decimal comparison %d", comparison)
	}
	words := (len(values) + 63) / 64
	if len(destination) < words {
		return 0, fmt.Errorf("hatSql: decimal bitmap has %d words, need %d", len(destination), words)
	}
	count := 0
	for wordIndex := 0; wordIndex < words; wordIndex++ {
		start := wordIndex * 64
		end := start + 64
		if end > len(values) {
			end = len(values)
		}
		var matches uint64
		for index := start; index < end; index++ {
			if comparison.matches(values[index].Compare(target)) {
				matches |= uint64(1) << uint(index-start)
			}
		}
		destination[wordIndex] = matches
		count += bits.OnesCount64(matches)
	}
	return count, nil
}

// FilterSQLDecimal256Batch compares values to target and packs matching row
// positions into destination. The caller owns destination and must provide at
// least (len(values)+63)/64 words. It performs no allocation on valid input.
func FilterSQLDecimal256Batch(values []SQLDecimal256, target SQLDecimal256, comparison SQLDecimalComparison, destination []uint64) (int, error) {
	if !comparison.valid() {
		return 0, fmt.Errorf("hatSql: invalid decimal comparison %d", comparison)
	}
	words := (len(values) + 63) / 64
	if len(destination) < words {
		return 0, fmt.Errorf("hatSql: decimal bitmap has %d words, need %d", len(destination), words)
	}
	count := 0
	for wordIndex := 0; wordIndex < words; wordIndex++ {
		start := wordIndex * 64
		end := start + 64
		if end > len(values) {
			end = len(values)
		}
		var matches uint64
		for index := start; index < end; index++ {
			if comparison.matches(values[index].Compare(target)) {
				matches |= uint64(1) << uint(index-start)
			}
		}
		destination[wordIndex] = matches
		count += bits.OnesCount64(matches)
	}
	return count, nil
}

func compareSQLDecimalFixedWords(left, right []byte) int {
	if len(left) != len(right) || len(left)%8 != 0 {
		return compareSQLDecimalFixedBytes(left, right)
	}
	leftNegative := len(left) != 0 && left[len(left)-1]&0x80 != 0
	rightNegative := len(right) != 0 && right[len(right)-1]&0x80 != 0
	if leftNegative != rightNegative {
		if leftNegative {
			return -1
		}
		return 1
	}
	for index := len(left) - 8; index >= 0; index -= 8 {
		leftWord := binary.LittleEndian.Uint64(left[index : index+8])
		rightWord := binary.LittleEndian.Uint64(right[index : index+8])
		if leftWord < rightWord {
			return -1
		}
		if leftWord > rightWord {
			return 1
		}
	}
	return 0
}

func compareSQLDecimalFixedBytes(left, right []byte) int {
	leftNegative := len(left) != 0 && left[len(left)-1]&0x80 != 0
	rightNegative := len(right) != 0 && right[len(right)-1]&0x80 != 0
	if leftNegative != rightNegative {
		if leftNegative {
			return -1
		}
		return 1
	}
	length := len(left)
	if len(right) > length {
		length = len(right)
	}
	leftExtension, rightExtension := byte(0), byte(0)
	if leftNegative {
		leftExtension = 0xff
	}
	if rightNegative {
		rightExtension = 0xff
	}
	for index := length - 1; index >= 0; index-- {
		leftByte, rightByte := leftExtension, rightExtension
		if index < len(left) {
			leftByte = left[index]
		}
		if index < len(right) {
			rightByte = right[index]
		}
		if leftByte < rightByte {
			return -1
		}
		if leftByte > rightByte {
			return 1
		}
	}
	return 0
}

func addSQLDecimalFixed(destination, left, right []byte) bool {
	var carry uint64
	for index := 0; index < len(destination); index += 8 {
		leftWord := binary.LittleEndian.Uint64(left[index : index+8])
		rightWord := binary.LittleEndian.Uint64(right[index : index+8])
		result, nextCarry := bits.Add64(leftWord, rightWord, carry)
		binary.LittleEndian.PutUint64(destination[index:index+8], result)
		carry = nextCarry
	}
	leftNegative := left[len(left)-1]&0x80 != 0
	rightNegative := right[len(right)-1]&0x80 != 0
	resultNegative := destination[len(destination)-1]&0x80 != 0
	return leftNegative == rightNegative && resultNegative != leftNegative
}

func subSQLDecimalFixed(destination, left, right []byte) bool {
	var borrow uint64
	for index := 0; index < len(destination); index += 8 {
		leftWord := binary.LittleEndian.Uint64(left[index : index+8])
		rightWord := binary.LittleEndian.Uint64(right[index : index+8])
		result, nextBorrow := bits.Sub64(leftWord, rightWord, borrow)
		binary.LittleEndian.PutUint64(destination[index:index+8], result)
		borrow = nextBorrow
	}
	leftNegative := left[len(left)-1]&0x80 != 0
	rightNegative := right[len(right)-1]&0x80 != 0
	resultNegative := destination[len(destination)-1]&0x80 != 0
	return leftNegative != rightNegative && resultNegative != leftNegative
}
