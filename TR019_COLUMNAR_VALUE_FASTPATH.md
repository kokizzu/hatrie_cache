# TR-019 Plain Columnar Value Fast Path

This is a contained physical-layout optimization inspired by Tarantool tuple
field access and ClickHouse vectorized columns.

`ColumnarBatch.Value` supports several physical representations. Before this
change, every lookup probed the optional dictionary, packed, boolean, numeric,
plain, list, and nested maps in sequence. Most legacy columnar batches use only
the plain `Columns` map, so those probes were unnecessary.

The fast path applies only when all optional physical-layout maps are `nil`. It
reads the plain column directly and keeps the existing specialized-layout
precedence for mixed batches. This is not the larger schema-aware tuple
field-offset cache proposed by TR-19; that remains open because public SQL rows
are still dynamic maps.

## Verification

The focused test covers plain lookup, missing fields, and specialized-layout
precedence. The benchmark uses 1,024 integer values and reports allocations.

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Before | 27.77; 28.44; 28.91; 29.98; 26.27 | 28.44 | 0 | 0 |
| After | 12.60; 12.01; 11.63; 12.06; 12.66 | 12.06 | 0 | 0 |

The median is **2.36x faster**, with unchanged zero allocations and no retained
memory. Commands:

```text
make test-tr019-columnar-value
make benchmark-tr019-columnar-value
```

The broader review also runs package tests, a race test, and `go vet` for the
affected packages.
