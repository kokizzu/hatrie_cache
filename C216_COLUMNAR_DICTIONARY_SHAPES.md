# C216: Lookup-Aware Columnar Dictionary Selection

This feature adds a public, opt-in selector for repeated string columns whose
dominant next operation is known by the caller. It is the ClickHouse-style
dictionary-layout idea applied at the columnar batch boundary.

## API

The existing method remains the compatibility default:

```go
batch.EncodeRepeatedStrings()
```

Code that knows the workload can select a layout explicitly:

```go
batch.EncodeRepeatedStringsForLookup(hatSql.ColumnarDictionaryLookupEquality)
```

The method changes only the representation of eligible all-string columns. It
preserves the existing `ColumnarBatch.Value` and SQL semantics, and it leaves
non-string, mixed, malformed, and high-cardinality columns unchanged.

## Admission Rules

The selector estimates the retained bytes for the plain and dictionary forms:

```text
plain      = rows * 16 + total string bytes
dictionary = rows * 4 + unique values * 16 + unique string bytes
```

All layouts still require at least four rows and at most roughly 75% unique
values. The estimates are bounded and use the same conservative cardinality
guard as the automatic encoder.

| Lookup shape | Dictionary admission |
| --- | --- |
| `Automatic` | Strictly smaller than the plain estimate. This is the behavior of `EncodeRepeatedStrings()`. |
| `Equality` | Smaller or exactly equal to the plain estimate. Dictionary codes are useful for equality probes. |
| `Grouping` | Smaller or exactly equal to the plain estimate. Grouping can compare and count codes without materializing keys. |
| `Ordering` | At least 10% smaller than the plain estimate. Ordered access usually needs dictionary-value indirection, so a marginal memory saving is rejected. |
| `Projection` | At least 10% smaller than the plain estimate. Generic value reads pay the same indirection cost. |
| Unknown value | Falls back to the strict automatic rule. |

The selector does not automatically infer a query shape, pack dictionary codes,
or change `HatTrie` and typed-table producer defaults. Those defaults remain
the legacy representation so existing layout caches, clones, and callers keep
their established behavior. A producer that owns an immutable batch and knows
that equality or grouping is the hot path may opt in before publishing it.

## Correctness Coverage

Focused tests cover:

- dictionary selection at the equality/grouping break-even boundary;
- the stricter ordering/projection threshold;
- unknown-shape fallback;
- preservation of every logical value after dictionary encoding.

## Measurement

Command:

```text
make benchmark-c216
```

The benchmark used Go's `-benchmem`, five samples, `-benchtime=200ms`, one
CPU, and 1,024 four-byte string values with 768 unique values. The estimated
retained layout is 20,480 bytes plain versus 19,456 bytes dictionary, or 1.05x
less for the dictionary form. This is a memory-admission result, not a claim
that every access path is faster.

### Raw Layout-Selection Samples

Each cell is `ns/op`; all dictionary rows used `113,464 B/op` and `32`
allocations, while plain ordering/projection rows used `112,312 B/op` and
`31` allocations.

| Shape | Sample 1 | Sample 2 | Sample 3 | Sample 4 | Sample 5 | Median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Automatic | 84,144 | 84,843 | 85,184 | 84,442 | 82,648 | 84,442 |
| Equality | 83,120 | 83,077 | 82,160 | 83,077 | 83,560 | 83,077 |
| Grouping | 82,400 | 83,414 | 82,781 | 82,886 | 82,676 | 82,781 |
| Ordering | 83,071 | 81,859 | 82,622 | 79,693 | 75,824 | 81,859 |
| Projection | 74,646 | 74,789 | 74,451 | 75,291 | 74,346 | 74,646 |

At this boundary, opting into a dictionary adds about 1% transient allocation
and one allocation during construction because of dictionary bookkeeping. The
retained layout estimate is lower, but the construction heap is not.

### Raw Generic Value-Read Samples

| Shape | Sample 1 | Sample 2 | Sample 3 | Sample 4 | Sample 5 | Median | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Automatic | 43,573 | 43,462 | 43,836 | 43,591 | 46,966 | 43,591 | 16,384 | 1,024 |
| Equality | 48,181 | 48,074 | 48,758 | 48,213 | 48,251 | 48,213 | 16,384 | 1,024 |
| Grouping | 47,910 | 44,597 | 45,055 | 43,635 | 44,642 | 44,642 | 16,384 | 1,024 |
| Ordering | 21,916 | 27,768 | 22,451 | 27,102 | 26,637 | 26,637 | 0 | 0 |
| Projection | 30,174 | 26,590 | 28,406 | 23,975 | 26,218 | 26,590 | 0 | 0 |

The generic `Value` benchmark is intentionally included as a guard against
overclaiming: dictionary reads are about 1.8x the plain ordering baseline for
the equality fixture and about 1.7x for grouping versus projection. SQL paths
that operate directly on dictionary codes can avoid this generic materializing
step; callers should use the selector only when that code-aware workload and
the retained-memory objective justify it.

## Decision

This remains additive and opt-in. The default automatic encoder is unchanged,
and no producer is silently switched to a representation whose generic read
benchmark is slower. The feature is useful as a safe building block for future
query-shape-aware producers, while its measured memory and CPU tradeoff is
explicit in this document.
