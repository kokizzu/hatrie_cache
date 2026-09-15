# Read Replica Selection Fast Paths

`T095c` keeps the existing read-consistency contract while reducing work in
two common selection cases:

- A single candidate is validated and returned directly after the consistency
  level has been checked.
- For multiple candidates with preferred regions, the selected candidate's
  region rank is cached instead of recomputed for every later candidate.

Candidate node trimming, blank-name rejection, required-frontier checks,
bounded-lag checks, locality preference, freshness, health, lexical
tie-breaking, and no-eligible error values remain unchanged. The input slice is
still not mutated. The singleton path also skips locality ranking because no
other candidate can outrank it.

## Verification

```text
make test-read-replica-policy-c213
make verify-read-replica-policy-c213
```

The verification target runs the full `hat/hatReplication` package, the race
detector, and `go vet`.

## Measurement

Command: `make benchmark-read-replica-policy-c213`.

Linux/amd64, AMD Ryzen 9 5950X, five samples per case, `-benchmem`:

| Workload | Before median | After median | Improvement | Before memory | After memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| One candidate | 27.43 ns/op | 18.47 ns/op | **1.49x faster** | 0 B/op, 0 allocs/op | 0 B/op, 0 allocs/op |
| Four candidates | 74.90 ns/op | 64.19 ns/op | **1.17x faster** | 0 B/op, 0 allocs/op | 0 B/op, 0 allocs/op |
| 1,024 ordinary candidates | 12,873 ns/op | 10,807 ns/op | **1.19x faster** | 0 B/op, 0 allocs/op | 0 B/op, 0 allocs/op |
| 1,024 preferred-region candidates | 32,584 ns/op | 20,022 ns/op | **1.63x faster** | 0 B/op, 0 allocs/op | 0 B/op, 0 allocs/op |

Raw samples are retained in [BENCHMARK.md](BENCHMARK.md#read-replica-selection-fast-path).
The benchmarks measure selection CPU only; network, transport, and replica
execution are outside their scope.
