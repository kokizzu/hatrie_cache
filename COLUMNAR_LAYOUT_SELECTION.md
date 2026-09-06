# Width-Aware Columnar Layout Selection

`ColumnarBatch.EncodeRepeatedStrings` now selects dictionary storage from an
estimated retained layout instead of using only a fixed unique-value ratio.
The estimate includes row count, interface slots, dictionary codes, string
headers, and the bytes owned by unique strings.

The selector keeps a conservative fixed-storage guard: dictionary codes and
unique string headers must remain smaller than the plain interface slice even
when repeated strings share backing storage. This allows wide repeated values
to benefit from dictionary encoding without making high-cardinality data pay a
larger retained representation.

The behavior is opt-in through the existing `EncodeRepeatedStrings` call used
by typed-table columnar layouts. Logical values and row order are unchanged;
`ColumnarBatch.Value` reads either physical representation identically.

## Measurement

Run:

```text
make benchmark-columnar-layout-selection
```

The focused benchmark on the development host measured:

| Input | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| 1,024 wide values, 700 unique | 137,391 | 191,097 | 35 |
| Six narrow unique values | 436 | 408 | 7 |

The first case now retains the compact dictionary representation. The
benchmark includes layout selection and construction, not only retained
memory, so use the real typed-table workload when sizing the end-to-end gain.
