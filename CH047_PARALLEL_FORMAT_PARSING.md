# CH-047 Parallel Format Parsing

`hatSql.ParseNDJSONParallel` is the opt-in parallel portion of the CH-047
format-parsing idea. It splits independent newline-delimited JSON records into
worker ranges, decodes them concurrently, and returns rows in source order.
`workers <= 0` uses the current `GOMAXPROCS` value, and the effective worker
count is capped by the number of input lines.

```go
rows, err := hatSql.ParseNDJSONParallel(data, 4)
if err != nil {
	return err
}
```

`ImportNDJSONParallel` uses the same parser and publishes the replacement
external-table snapshot only after every record succeeds. Blank lines are
ignored, non-object values are rejected, and when multiple records are invalid
the lowest source line is reported deterministically. The parser returns owned
maps and does not retain the input byte slice.

This is intentionally additive. The existing streaming JSONEachRow and CSV
APIs remain unchanged, including their bounded reader options and callback
semantics. CSV is not parallelized here because RFC 4180 quoted newlines make
naive line partitioning unsafe; a future CSV implementation needs a framing
pass before workers can decode records safely.

## Measurement

Five `-benchmem` samples were run on Linux/amd64 using an AMD Ryzen 9 5950X,
20,000 JSON objects, and four parallel workers. The serial control uses the
same `bytes.Split` plus `json.Unmarshal` workload without worker goroutines.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Serial control | 23,454,336 | 12,647,249 | 279,995 | 1.00x |
| `ParseNDJSONParallel(..., 4)` | 10,028,278 | 13,139,247 | 280,004 | 2.34x faster |

The parallel path uses about 492 KB more transient allocation per operation
and nine additional allocations. It remains opt-in because small inputs and
single-worker callers can prefer the serial path; the default streaming and
import APIs are unchanged.

Run the focused checks with:

```text
make test-ch047-parallel
make race-ch047-parallel
make benchmark-ch047-parallel
```
