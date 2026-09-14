# Columnar Bloom Filters

The columnar cache keeps a fixed 1,024-bit Bloom mark for each 256-row string
segment when the layout cache can build one. It is a pruning hint, not an
index: a Bloom miss skips a segment, while a Bloom hit still performs exact
row-level SQL comparison.

Literal binary-collation `field IN ('a', 'b', ...)` predicates use all literal
probes together. A segment is skipped only when every probe is absent. Duplicate
literals do not duplicate result rows, and NULL or non-string values retain
normal SQL behavior. Missing or malformed sidecars, non-binary collations,
non-literal expressions, and wider conjunctions use the established executor.

The feature is automatic for eligible cached columnar layouts and does not
change the wire or persistence format. Each populated segment mark is 128
bytes; no mark is allocated per row. False positives cost a normal scan, but
false negatives are impossible because the final comparison remains exact.

See the raw before/after measurements in
[BENCHMARK.md](BENCHMARK.md#clickhouse-style-string-bloom-literal-in).
