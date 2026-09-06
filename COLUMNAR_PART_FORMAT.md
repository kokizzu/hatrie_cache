# Columnar Part Format Selection

`hatSql.SelectSQLColumnarPartFormat` gives storage adapters a small policy
boundary for choosing ClickHouse-style compact or wide organization. Compact is
selected only when both the row count and estimated encoded byte width are at
or below policy limits; a part that exceeds either limit is wide.

```go
policy := hatSql.DefaultSQLColumnarPartFormatPolicy()
format, err := hatSql.SelectSQLColumnarPartFormat(
    int64(rowCount),
    estimatedEncodedBytes,
    policy,
)
```

The default policy is:

| Limit | Default |
| --- | ---: |
| Maximum compact rows | 10,000 |
| Maximum compact bytes | 1 MiB |

Both boundaries are inclusive. Storage adapters can provide a custom
`SQLColumnarPartFormatPolicy` when their filesystem, column count, or merge
workload justifies a different threshold. Negative input measurements and
non-positive policy limits are rejected. The selector allocates nothing and
does not inspect or mutate part data.

Compact organization is useful for small parts because it avoids many small
files or streams. Wide organization is preferable for larger parts because
readers can seek and merge individual columns without touching unrelated data.
This API chooses the layout; the caller remains responsible for writing,
reading, checksumming, and migrating the physical representation.

The decision is intentionally an AND policy rather than a row-only heuristic:
a small number of very wide rows still uses wide storage, and a narrow part
with too many rows does too. This bounds both metadata overhead and compact
merge working sets without affecting query correctness.

Focused coverage is in `hat/hatSql/columnar_part_format_test.go`, including
threshold boundaries, each rejection path, custom policies, and an
allocation-reporting benchmark. The benchmark measures the selector itself,
not filesystem I/O or part encoding.
