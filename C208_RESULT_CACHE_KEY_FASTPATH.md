# C208 Result-Cache Key Byte Fastpath

The SQL result-cache key already used the gob encoding of query parameters. The
hot path now appends those encoded bytes directly to the key builder instead of
converting them to an intermediate string first. The key prefix, length
framing, parameter encoding, cache eligibility, and settings-fingerprint
namespace are unchanged.

## Measured Result

Linux/amd64, AMD Ryzen 9 5950X, paired Go benchmark `-benchmem -count=12`:

| Workload | String baseline median | Byte fastpath median | Improvement | Before memory | After memory | Before allocs | After allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Paired parameterized key | 2,289 ns/op | 2,262 ns/op | 1.01x faster | 1,683 B/op | 1,619 B/op | 24 | 23 |

The end-to-end cached-query benchmark kept the same `2,091 allocs/op` and
approximately `362 KiB/op`; this optimization targets key construction and
does not change result cloning or cache storage.

Raw paired samples (`string-baseline`, then `byte-fastpath`, ns/op):

```text
2281 2291 2439 2323 2273 2286 2285 2268 2358 2412 2458 2272
2258 2270 2225 2200 2261 2262 2264 2273 2346 2295 2255 2261
```

## Verification

- `make test-c208-key-worktree`
- `make race-c208-key-worktree`
- Existing C208 result-cache benchmarks

The full C190 `hat/hatSql` suite remains blocked by pre-existing Decimal/Enum/IP
bitmap failures reproduced on an untouched baseline worktree.
