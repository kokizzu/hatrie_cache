# CH-051 Low-Cardinality String Columns

This is a ClickHouse-style, opt-in dictionary encoding for repeated string
columns. `LowCardinalityStringColumn` stores one sorted dictionary and one
packed code per row. A dictionary with up to 256 values uses one byte per row;
up to 65,536 values uses two bytes; larger dictionaries use four bytes.

The sorted codes preserve lexical order. That makes a code usable for equality
filters and `GROUP BY`, and lets an `ORDER BY` implementation compare codes
without materializing the row strings. `CountCodes` is the dense aggregation
path when the full grouped count is wanted.

## Usage

```go
builder := hatDataStructure.NewLowCardinalityStringBuilder(
	hatDataStructure.LowCardinalityStringBuilderOptions{
		InitialCapacity:    64,
		InitialRowCapacity: 100_000,
		MaxDistinctValues:  256,
	},
)
for _, region := range regions {
	if _, err := builder.Append(region); err != nil {
		return err
	}
}
column, err := builder.Build()
if err != nil {
	return err
}

code, present := column.LookupCode("ap-southeast-1")
counts := column.CountCodes()
_ = code
_ = present
_ = counts
```

`AppendNull` adds a NULL without adding a dictionary value. `Build` sorts the
dictionary, remaps row codes, releases the builder hash map, and returns an
immutable column. `MaxDistinctValues == 0` selects the conservative default of
4,096 values. Set it to a positive workload-specific limit when the caller
knows the column's expected cardinality; set it to `-1` only after measuring a
workload that needs an unbounded dictionary.

`MarshalBinary` writes a deterministic bounded `HLC1` representation. The
decoder rejects unsupported versions, unsorted dictionaries, invalid codes,
truncated data, trailing bytes, impossible NULL state, and oversized counts.
It is suitable for controlled persistence or peer transfer, subject to the
same input trust and key-management rules as other binary formats.

This is a reusable data-structure building block. It is not an automatic SQL
planner choice yet: a typed SQL source should select it only when statistics or
an explicit schema contract show that the string column is low-cardinality.

## Measurements

The baseline uses ordinary independent Go strings and either a string map or a
plain length-prefixed string encoding. The encoded column uses packed codes.
All figures are medians of five local runs on the same machine; the exact raw
samples are below.

| Workload | Baseline | Encoded | Improvement | Retained/memory result |
| --- | ---: | ---: | ---: | ---: |
| Build, 100k rows / 64 values | 0.985 ms/op | 1.859 ms/op | 1.89x slower | 2.80 MB -> 101.8 KB, 27.5x lower |
| GROUP BY string map | 1.419 ms/op | 2.538 ms/op with per-row code map | 1.79x slower | 9,297 -> 2,344 B/op, 4.0x lower |
| GROUP BY dense code counts | string-map baseline above | 0.802 ms/op | 1.77x faster | 9,297 -> 512 B/op, 18.2x lower |
| Binary encode, 100k rows / 64 values | 0.985 ms/op | 0.870 ms/op | 1.13x faster | 1,300,000 -> 100,842 wire bytes, 12.9x lower |
| High cardinality build, 20k rows / 20k values | 0.196 ms/op | 1.888 ms/op | 9.65x slower | 560 KB -> 600 KB retained; 6.8x more B/op |

The feature is therefore not a universal string replacement. Its good case is
repeated values with dense grouping or bandwidth/storage pressure. The default
distinct limit and the explicit opt-in API keep the high-cardinality loss out
of existing paths.

Commands:

- `make benchmark-ch051-build-stable-c203`
- `make benchmark-ch051-group-stable-c203`
- `make benchmark-ch051-wire-stable-c203`
- `make benchmark-ch051-high-cardinality-stable-c203`

Raw samples:

```text
BenchmarkLowCardinalityStringBuild: 1858813, 1901474, 1857883, 1751529, 1943276 ns/op; 101792 retained-bytes; 523613, 524526, 524434, 523455, 523764 B/op
BenchmarkPlainStringColumnBuild: 859160, 870517, 985182, 1072726, 1089007 ns/op; 2800000 retained-bytes; 1609235, 1608611, 1609527, 1609256, 1609518 B/op
BenchmarkLowCardinalityStringGroupByCodes: 2442498, 2451851, 2537921, 2543548, 2569825 ns/op; 2344 B/op; 3 allocs/op
BenchmarkPlainStringGroupByValues: 1442147, 1483385, 1364115, 1378225, 1418537 ns/op; 9758, 9297, 9222, 9263, 9397 B/op; 263, 243, 240, 242, 248 allocs/op
BenchmarkLowCardinalityStringGroupByDenseCounts: 864329, 802308, 765479, 747968, 830239 ns/op; 512 B/op; 1 alloc/op
BenchmarkLowCardinalityStringMarshal: 877881, 812092, 870134, 834472, 879792 ns/op; 100842 wire-bytes; 123834, 124019, 123566, 124268, 124146 B/op
BenchmarkPlainStringMarshal: 984740, 1063662, 976450, 1065562, 956558 ns/op; 1300000 wire-bytes; 2806310, 2806589, 2805275, 2806728, 2806014 B/op
BenchmarkLowCardinalityStringHighCardinalityBuild: 1887853, 2002152, 1860396, 2047199, 1799605 ns/op; 600000 retained-bytes; 2219342, 2219380, 2219288, 2219376, 2219241 B/op
BenchmarkPlainStringHighCardinalityBuild: 164623, 197125, 206837, 195556, 186092 ns/op; 560000 retained-bytes; 327778, 327860, 327854, 327874, 327871 B/op
```
