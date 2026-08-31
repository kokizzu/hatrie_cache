# Columnar N-Gram Sidecars

Large warmed plain-string columnar layouts can now retain a fixed 1,024-bit
three-gram Bloom filter per 256-row segment. The SQL fast path uses it only for
literal substring predicates shaped exactly as `LIKE '%text%'`, where `text`
is at least three bytes and has no wildcard or escape character.

The sidecar can prove that a segment cannot contain the literal, then the
ordinary `LIKE` evaluator checks every retained candidate. It therefore has no
false negatives and preserves SQL results. Other `LIKE` patterns, regular
expressions, cold layouts, small layouts, and unavailable sidecars retain the
current scan behavior.

## Activation And Cost

No service worker or configuration is required. `HatTrie` creates the sidecar
only after its existing columnar layout cache is warmed and only for a
high-cardinality plain-string layout with at least 4,096 rows. Dictionary
columns already execute `LIKE` by exact dictionary code and do not allocate an
n-gram sidecar.

Each sidecar is 128 bytes per 256-row segment. A 20,000-row plain-string field
uses 79 segments, or about 10 KiB plus map metadata. This is retained only in
the bounded layout cache. The existing equality Bloom filter remains separate,
so qualifying string-layout sidecar memory roughly doubles.

## Measurement

On an AMD Ryzen 9 5950X, `make benchmark-sql-columnar-ngram` measured the
median of three 20,000-row selective literal-substring queries:

| Layout | Time/op | Heap/op | Allocs/op |
| --- | ---: | ---: | ---: |
| With n-gram segments | 9.82 us | 5,533 B | 86 |
| Same layout without n-grams | 2.58 ms | 1.29 MB | 40,024 |

For this selective workload, the sidecar is approximately `263x` faster,
uses `232x` less heap, and performs `465x` fewer allocations. Broad matches
and short or wildcard patterns may not skip segments and retain the ordinary
path.

## Verification

```sh
make test-sql-columnar-ngram
make benchmark-sql-columnar-ngram
```
