package hatSql

import (
	"math/big"
	"math/rand"
	"strconv"
	"testing"
)

func mustSQLDecimal128KernelValue(t *testing.T, value string) SQLDecimal128 {
	t.Helper()
	parsed, err := ParseSQLDecimal128(value, 0)
	if err != nil {
		t.Fatalf("ParseSQLDecimal128(%q) error = %v", value, err)
	}
	return parsed
}

func mustSQLDecimal256KernelValue(t *testing.T, value string) SQLDecimal256 {
	t.Helper()
	parsed, err := ParseSQLDecimal256(value, 0)
	if err != nil {
		t.Fatalf("ParseSQLDecimal256(%q) error = %v", value, err)
	}
	return parsed
}

func TestSQLDecimal128KernelCompareAndArithmetic(t *testing.T) {
	left := mustSQLDecimal128KernelValue(t, "123456789012345678901234567890")
	right := mustSQLDecimal128KernelValue(t, "-98765432109876543210")
	if got := left.Compare(right); got <= 0 {
		t.Fatalf("SQLDecimal128.Compare() = %d, want positive", got)
	}

	added, overflow := AddSQLDecimal128(left, right)
	if overflow {
		t.Fatal("AddSQLDecimal128() overflow = true, want false")
	}
	if formatted, err := added.Format(0); err != nil || formatted != "123456788913580246791358024680" {
		t.Fatalf("AddSQLDecimal128() = %q/%v, want exact sum", formatted, err)
	}

	subtracted, overflow := SubSQLDecimal128(left, right)
	if overflow {
		t.Fatal("SubSQLDecimal128() overflow = true, want false")
	}
	if formatted, err := subtracted.Format(0); err != nil || formatted != "123456789111111111011111111100" {
		t.Fatalf("SubSQLDecimal128() = %q/%v, want exact difference", formatted, err)
	}
}

func TestSQLDecimal256KernelCompareAndArithmetic(t *testing.T) {
	left := mustSQLDecimal256KernelValue(t, "1234567890123456789012345678901234567890")
	right := mustSQLDecimal256KernelValue(t, "-987654321098765432109876543210")
	if got := left.Compare(right); got <= 0 {
		t.Fatalf("SQLDecimal256.Compare() = %d, want positive", got)
	}

	added, overflow := AddSQLDecimal256(left, right)
	if overflow {
		t.Fatal("AddSQLDecimal256() overflow = true, want false")
	}
	if formatted, err := added.Format(0); err != nil || formatted != "1234567889135802467913580246791358024680" {
		t.Fatalf("AddSQLDecimal256() = %q/%v, want exact sum", formatted, err)
	}

	subtracted, overflow := SubSQLDecimal256(left, right)
	if overflow {
		t.Fatal("SubSQLDecimal256() overflow = true, want false")
	}
	if formatted, err := subtracted.Format(0); err != nil || formatted != "1234567891111111110111111111011111111100" {
		t.Fatalf("SubSQLDecimal256() = %q/%v, want exact difference", formatted, err)
	}
}

func TestSQLDecimalKernelOverflowMatchesSignedWidth(t *testing.T) {
	max128 := mustSQLDecimal128KernelValue(t, "170141183460469231731687303715884105727")
	one128 := mustSQLDecimal128KernelValue(t, "1")
	if _, overflow := AddSQLDecimal128(max128, one128); !overflow {
		t.Fatal("AddSQLDecimal128(max, 1) overflow = false, want true")
	}
	min128 := mustSQLDecimal128KernelValue(t, "-170141183460469231731687303715884105728")
	if _, overflow := SubSQLDecimal128(min128, one128); !overflow {
		t.Fatal("SubSQLDecimal128(min, 1) overflow = false, want true")
	}

	max256 := mustSQLDecimal256KernelValue(t, "57896044618658097711785492504343953926634992332820282019728792003956564819967")
	one256 := mustSQLDecimal256KernelValue(t, "1")
	if _, overflow := AddSQLDecimal256(max256, one256); !overflow {
		t.Fatal("AddSQLDecimal256(max, 1) overflow = false, want true")
	}
}

func TestSQLDecimal128BatchFilterPacksMatches(t *testing.T) {
	values := make([]SQLDecimal128, 70)
	for index := range values {
		values[index] = mustSQLDecimal128KernelValue(t, strconv.Itoa(index-35))
	}
	target := mustSQLDecimal128KernelValue(t, "0")
	bitmap := make([]uint64, 2)
	count, err := FilterSQLDecimal128Batch(values, target, SQLDecimalGreaterOrEqual, bitmap)
	if err != nil {
		t.Fatalf("FilterSQLDecimal128Batch() error = %v", err)
	}
	if count != 35 || bitmap[0] != 0xfffffff800000000 || bitmap[1] != 0x3f {
		t.Fatalf("FilterSQLDecimal128Batch() = count %d bitmap %#x, want count 35 bitmap [0xfffffff800000000 0x3f]", count, bitmap)
	}
}

func TestSQLDecimal256BatchFilterMatchesScalarComparison(t *testing.T) {
	values := []SQLDecimal256{
		mustSQLDecimal256KernelValue(t, "-2"),
		mustSQLDecimal256KernelValue(t, "-1"),
		mustSQLDecimal256KernelValue(t, "0"),
		mustSQLDecimal256KernelValue(t, "1"),
		mustSQLDecimal256KernelValue(t, "2"),
	}
	bitmap := make([]uint64, 1)
	count, err := FilterSQLDecimal256Batch(values, mustSQLDecimal256KernelValue(t, "0"), SQLDecimalLess, bitmap)
	if err != nil {
		t.Fatalf("FilterSQLDecimal256Batch() error = %v", err)
	}
	if count != 2 || bitmap[0] != 0x3 {
		t.Fatalf("FilterSQLDecimal256Batch() = count %d bitmap %#x, want count 2 bitmap 0x3", count, bitmap[0])
	}
}

func TestSQLDecimal128BatchFilterSupportsEveryComparison(t *testing.T) {
	values := []SQLDecimal128{
		mustSQLDecimal128KernelValue(t, "-1"),
		mustSQLDecimal128KernelValue(t, "0"),
		mustSQLDecimal128KernelValue(t, "1"),
	}
	target := mustSQLDecimal128KernelValue(t, "0")
	wants := map[SQLDecimalComparison]uint64{
		SQLDecimalEqual:          0x2,
		SQLDecimalNotEqual:       0x5,
		SQLDecimalLess:           0x1,
		SQLDecimalLessOrEqual:    0x3,
		SQLDecimalGreater:        0x4,
		SQLDecimalGreaterOrEqual: 0x6,
	}
	for comparison, wantBitmap := range wants {
		bitmap := make([]uint64, 1)
		count, err := FilterSQLDecimal128Batch(values, target, comparison, bitmap)
		if err != nil {
			t.Fatalf("comparison %d error = %v", comparison, err)
		}
		if bitmap[0] != wantBitmap || count != bitsSetInSQLDecimalKernelTest(wantBitmap) {
			t.Fatalf("comparison %d = count %d bitmap %#x, want count %d bitmap %#x", comparison, count, bitmap[0], bitsSetInSQLDecimalKernelTest(wantBitmap), wantBitmap)
		}
	}
}

func TestSQLDecimalBatchFilterValidatesOutputAndComparison(t *testing.T) {
	values := []SQLDecimal128{mustSQLDecimal128KernelValue(t, "1")}
	if _, err := FilterSQLDecimal128Batch(values, values[0], SQLDecimalEqual, nil); err == nil {
		t.Fatal("FilterSQLDecimal128Batch() with short output error = nil, want error")
	}
	if _, err := FilterSQLDecimal128Batch(values, values[0], SQLDecimalComparison(99), make([]uint64, 1)); err == nil {
		t.Fatal("FilterSQLDecimal128Batch() with invalid comparison error = nil, want error")
	}
}

func TestSQLDecimal128KernelsMatchRandomSignedReference(t *testing.T) {
	random := rand.New(rand.NewSource(128))
	for iteration := 0; iteration < 512; iteration++ {
		var left, right SQLDecimal128
		if _, err := random.Read(left[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := random.Read(right[:]); err != nil {
			t.Fatal(err)
		}
		leftReference := sqlDecimalBigInt(left[:], 128)
		rightReference := sqlDecimalBigInt(right[:], 128)
		if got, want := left.Compare(right), leftReference.Cmp(rightReference); got != want {
			t.Fatalf("iteration %d Compare() = %d, want %d", iteration, got, want)
		}

		checkSQLDecimal128ArithmeticReference(t, iteration, left, right, false)
		checkSQLDecimal128ArithmeticReference(t, iteration, left, right, true)
	}
}

func TestSQLDecimal256KernelsMatchRandomSignedReference(t *testing.T) {
	random := rand.New(rand.NewSource(256))
	for iteration := 0; iteration < 512; iteration++ {
		var left, right SQLDecimal256
		if _, err := random.Read(left[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := random.Read(right[:]); err != nil {
			t.Fatal(err)
		}
		leftReference := sqlDecimalBigInt(left[:], 256)
		rightReference := sqlDecimalBigInt(right[:], 256)
		if got, want := left.Compare(right), leftReference.Cmp(rightReference); got != want {
			t.Fatalf("iteration %d Compare() = %d, want %d", iteration, got, want)
		}

		checkSQLDecimal256ArithmeticReference(t, iteration, left, right, false)
		checkSQLDecimal256ArithmeticReference(t, iteration, left, right, true)
	}
}

func checkSQLDecimal128ArithmeticReference(t *testing.T, iteration int, left, right SQLDecimal128, subtract bool) {
	t.Helper()
	leftReference := sqlDecimalBigInt(left[:], 128)
	rightReference := sqlDecimalBigInt(right[:], 128)
	want := new(big.Int)
	if subtract {
		want.Sub(leftReference, rightReference)
	} else {
		want.Add(leftReference, rightReference)
	}
	minimum, maximum := sqlDecimalSignedRange(128)
	wantOverflow := want.Cmp(minimum) < 0 || want.Cmp(maximum) > 0
	var got SQLDecimal128
	var gotOverflow bool
	if subtract {
		got, gotOverflow = SubSQLDecimal128(left, right)
	} else {
		got, gotOverflow = AddSQLDecimal128(left, right)
	}
	if gotOverflow != wantOverflow {
		t.Fatalf("iteration %d arithmetic overflow = %t, want %t", iteration, gotOverflow, wantOverflow)
	}
	assertSQLDecimalFixedReference(t, iteration, got[:], want, 128)
}

func checkSQLDecimal256ArithmeticReference(t *testing.T, iteration int, left, right SQLDecimal256, subtract bool) {
	t.Helper()
	leftReference := sqlDecimalBigInt(left[:], 256)
	rightReference := sqlDecimalBigInt(right[:], 256)
	want := new(big.Int)
	if subtract {
		want.Sub(leftReference, rightReference)
	} else {
		want.Add(leftReference, rightReference)
	}
	minimum, maximum := sqlDecimalSignedRange(256)
	wantOverflow := want.Cmp(minimum) < 0 || want.Cmp(maximum) > 0
	var got SQLDecimal256
	var gotOverflow bool
	if subtract {
		got, gotOverflow = SubSQLDecimal256(left, right)
	} else {
		got, gotOverflow = AddSQLDecimal256(left, right)
	}
	if gotOverflow != wantOverflow {
		t.Fatalf("iteration %d arithmetic overflow = %t, want %t", iteration, gotOverflow, wantOverflow)
	}
	assertSQLDecimalFixedReference(t, iteration, got[:], want, 256)
}

func assertSQLDecimalFixedReference(t *testing.T, iteration int, got []byte, want *big.Int, width int) {
	t.Helper()
	modulus := new(big.Int).Lsh(big.NewInt(1), uint(width))
	wrapped := new(big.Int).Mod(new(big.Int).Set(want), modulus)
	if wrapped.Sign() < 0 {
		wrapped.Add(wrapped, modulus)
	}
	half := new(big.Int).Rsh(new(big.Int).Set(modulus), 1)
	if wrapped.Cmp(half) >= 0 {
		wrapped.Sub(wrapped, modulus)
	}
	if gotReference := sqlDecimalBigInt(got, width); gotReference.Cmp(wrapped) != 0 {
		t.Fatalf("iteration %d wrapped coefficient = %s, want %s", iteration, gotReference, wrapped)
	}
}

func bitsSetInSQLDecimalKernelTest(value uint64) int {
	count := 0
	for value != 0 {
		value &= value - 1
		count++
	}
	return count
}
