# CH-U14 Spill Runtime Join Filter

Hatrie now applies the existing opt-in `SpillBloom` filters inside the
bounded spill hash join. The implementation is deliberately scoped to the
direct two-source `INNER` equality join that already supports spill files.

## Behavior

- `QueryOptions.SpillBloom` remains `false` by default.
- The existing partition Bloom filters still skip partition pairs that cannot
  share a join key.
- When the right input has more rows than the left input, the right spill
  stream is probed against the left partition Bloom filter before rows enter a
  right-side chunk hash table.
- Bloom misses only skip work. Every Bloom hit still goes through the exact
  string-key hash table, so false positives cannot change results.
- `EXPLAIN ANALYZE` reports a `RUNTIME JOIN FILTER` step with probe and skip
  counts when the build-side filter is active.
- Unsupported query shapes and the ordinary in-memory path are unchanged.

Example:

```go
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.QueryOptions{
    JoinOverflowPolicy: hatSql.SQLJoinOverflowSpill,
    MaxJoinBytes:       256 << 10,
    SpillDirectory:     "/var/lib/hatrie/spill",
    MaxSpillBytes:      4 << 30,
    SpillBloom:         true,
})
```

The spill directory must still be private to the service and governed by the
existing quota, permissions, cleanup, and encryption settings. The Bloom
filter contains only hashed join-key membership bits; it is not a substitute
for access control or spill-file encryption.

## Measurement

Command:

```text
make benchmark-chu14
```

Machine: Linux amd64, AMD Ryzen 9 5950X. Each case used five benchmark
samples, 4096 rows on one side, 32 rows on the other side, 32 matching keys,
`MaxJoinBytes=256`, and `MaxSpillBytes=64 MiB`.

Right-heavy case, median of the raw samples:

| Configuration | ns/op | B/op | allocs/op | Relative CPU | Relative memory | Relative allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Before, partition Bloom | 38,524,238 | 8,697,498 | 167,452 | 1.00x | 1.00x | 1.00x |
| After, build-side runtime filter | 22,629,966 | 3,157,454 | 74,230 | 1.70x faster | 2.75x lower | 2.26x lower |

The raw after samples were `23,366,127`, `22,629,966`, `22,391,905`,
`22,282,911`, and `25,631,339 ns/op`; their median is `22,629,966 ns/op`.
The raw before samples were
`38,524,238`, `38,380,133`, `36,596,143`, `39,985,481`, and `38,713,786
ns/op`; their median is `38,524,238 ns/op`.

The benchmark also includes a left-heavy case. The new filter is disabled
there because the right input is already smaller; this avoids paying the
build-side filter cost where it is unlikely to reduce work. The option is
still opt-in because Bloom probes consume CPU and can be counterproductive for
balanced or highly matching inputs.

## Verification

- `make test-chu14` checks result equivalence, selective filtering, plan
  visibility, spill cleanup, and exact row counts.
- Existing C229 spill-policy tests remain the compatibility guard for reject,
  spill, unsupported shapes, and cleanup behavior.
- The first attempted left-side per-key probe was measured and discarded: it
  added CPU without reducing the already-cheap exact map lookup on the
  benchmark workload.
