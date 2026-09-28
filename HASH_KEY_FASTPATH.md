# Typed Hash-Key Fast Path

`hatHash.FNV1a64Uint64` and `hatHash.FNV1a64Int64` hash numeric keys directly,
without first allocating or filling an intermediate byte slice. Both functions
use canonical big-endian bytes, so their result is exactly the same as
`FNV1a64` over an eight-byte big-endian encoding of the value.

These helpers apply a shared idea from the compared systems: keep typed keys in
their compact representation until the hash/index boundary. ClickHouse uses
typed hash keys in grouped execution, Materialize keeps typed arrangement keys,
and Tarantool's numeric indexes avoid converting numeric keys to text. The API
is intentionally small and independent of any table or wire format.

```go
numericHash := hatHash.FNV1a64Uint64(id)
signedHash := hatHash.FNV1a64Int64(sequence)
```

The functions are deterministic but non-cryptographic. They are suitable for
partitioning, hash tables, Bloom filters, and sketches where the existing FNV
helpers are already appropriate. They must not be used for authentication or
untrusted hash-flooding defenses that require a secret-seeded hash.

## Measurement

Command:

```text
make benchmark-hash-key-fastpath
```

Machine: AMD Ryzen 9 5950X, linux/amd64, Go benchmark with `-benchmem -count=5`.
The values below are the five samples from one run; the comparison uses the
median.

| Path | Samples (ns/op) | Median | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing `binary.BigEndian.PutUint64` plus `FNV1a64([]byte)` | 6.856, 6.431, 6.213, 6.272, 7.024 | 6.431 | 0 | 0 |
| `FNV1a64Uint64` | 3.650, 3.544, 3.486, 3.348, 3.658 | 3.544 | 0 | 0 |
| `FNV1a64Int64` | 3.571, 3.458, 3.736, 3.598, 3.567 | 3.571 | 0 | 0 |

The unsigned path is `1.81x` faster on this workload. Memory usage and
allocation count are unchanged because the baseline already used a stack array;
the win is CPU work and a simpler call site. Existing byte-oriented APIs remain
unchanged, so callers that require a different byte order or hash framing keep
using them explicitly.

Verification:

```text
make test-hash-key-fastpath
make test-hash-package
make race-hash-key-fastpath
```
