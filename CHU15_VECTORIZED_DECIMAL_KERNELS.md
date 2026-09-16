# CH-U15 Fixed-Width Decimal Kernels

Hatrie Cache now has allocation-free fixed-width kernels for the existing
`SQLDecimal128` and `SQLDecimal256` coefficient types.

## What Changed

- `SQLDecimal128.Compare` and `SQLDecimal256.Compare` compare signed
  two's-complement coefficients in 64-bit little-endian words.
- `AddSQLDecimal128`, `AddSQLDecimal256`, `SubSQLDecimal128`, and
  `SubSQLDecimal256` use `math/bits` carry/borrow operations and return a
  signed-width overflow flag.
- `FilterSQLDecimal128Batch` and `FilterSQLDecimal256Batch` evaluate a fixed
  comparison against a caller-owned `[]uint64` bitmap.
- The existing RowBinary decimal statistics comparison uses the word kernel,
  so existing pruning behavior benefits without a format or configuration
  change.

The arithmetic functions operate on coefficients with the same scale. They
return the wrapped fixed-width coefficient together with `overflow=true`; a
caller that needs SQL error semantics must reject that result. They do not
perform scale alignment, rounding, multiplication, or division.

The implementation is pure Go and portable across architectures. It avoids
`math/big`, unsafe code, architecture-specific assembly, and hidden output
allocation. The batch filter requires `(len(values)+63)/64` destination words;
for 1,024 rows that is 128 bytes, versus 1,024 bytes for one boolean per row.

## API Example

```go
left, _ := hatSql.ParseSQLDecimal128("1200", 0)
right, _ := hatSql.ParseSQLDecimal128("25", 0)
sum, overflow := hatSql.AddSQLDecimal128(left, right)
if overflow {
	// Reject according to the application's SQL overflow policy.
}

bitmap := make([]uint64, (len(values)+63)/64)
matched, err := hatSql.FilterSQLDecimal128Batch(
	values,
	left,
	hatSql.SQLDecimalGreaterOrEqual,
	bitmap,
)
```

## Benchmark

The clean baseline was `HEAD` before CH-U15. The after run used the same
benchmark workload plus the word kernel. Each cell is the range of five
200-ms samples for 1,024 values on an AMD Ryzen 9 5950X; lower `ns/op` is
better. Both baseline and kernel paths reported `0 B/op` and `0 allocs/op`.

| Operation | Clean baseline ns/op | Word kernel ns/op | Median speedup |
| --- | ---: | ---: | ---: |
| Compare Decimal128 | 6,295–7,564 | 3,480–3,952 | 1.90x |
| Compare Decimal256 | 9,660–11,757 | 4,234–5,063 | 2.36x |
| Add Decimal128 | 8,948–10,910 | 7,393–8,496 | 1.27x |
| Add Decimal256 | 21,263–24,210 | 10,105–11,988 | 1.94x |
| Filter Decimal128 | 7,033–8,403 | 4,627–5,781 | 1.35x |
| Filter Decimal256 | 10,336–12,351 | 5,375–6,411 | 1.75x |

The after run also included bytewise controls in the same process. Their
median versus the word kernel was 1.65x/2.17x for 128/256-bit comparison,
1.44x/2.03x for addition, and 1.38x/1.89x for packed filtering. This controls
for machine noise; the clean-HEAD comparison is the reported change baseline.

Raw five-sample outputs are retained in the benchmark command output. Re-run
them with:

```text
make benchmark-chu15-before-c255
make benchmark-chu15-c255
```

The correctness target exercises existing decimal round trips plus signed
boundary cases, overflow, all packed comparison validation, and 512 random
128-bit and 256-bit add/subtract/compare cases against `math/big` references:

```text
make test-chu15-c255
```
