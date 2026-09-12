# Journal Replay

`CommandJournal.Replay` remains the default recovery API and keeps its
serialized ordering and public behavior. It now uses a private apply path for
the common `SET`, `SETX`, `SETINT`, `SETINTX`, `INC`, and `DEL` mutations. The
path preserves the command transaction lock, validation, expiration handling,
overflow checks, and error propagation, but does not allocate a
`CacheCommandResponse` that recovery never returns. All other commands use the
existing command dispatcher.

Parallel replay was measured separately and rejected: the trie-level lock made
four keyed workers slower than ordered replay and increased memory use. It is
therefore still tracked as T042 rather than being presented as implemented.

## Measurement

Workload: one `SETINT` followed by 4,096 `INC` mutations. The direct apply
comparison used the same requests and was measured with Go benchmarks on an
AMD Ryzen 9 5950X.

| Path | Time | Allocations | Bytes |
| --- | ---: | ---: | ---: |
| Public command API | 1.09 ms | 4,005 | 18,677 B |
| Replay apply fast path | 0.87 ms | 8 | 3,412 B |

End-to-end journal replay, including journal decoding and scanning, used a
4,097-record journal:

| Path | Time | Allocations | Bytes |
| --- | ---: | ---: | ---: |
| Existing command-API replay | 7.93 ms | 86,042 | 3,694,129 B |
| Replay with fast apply | 7.61 ms | 81,959 | 3,694,025 B |

The end-to-end result is approximately 1.04x faster, with 4.7% fewer
allocations and effectively unchanged total bytes. The focused command-path
result is larger because journal decoding remains the dominant cost for a full
recovery.

## Cached Replay Metadata

The journal already validates its complete tail and checkpoint boundary while
opening. In-process checkpoint writes and compaction update the same cached
metadata. Ordinary `Replay` and `ReplayThrough` reuse those values while the
journal mutex is held, so they no longer rescan the journal once to discover
metadata and then scan it again to apply commands. `ReplayWithProgress` still
performs the discovery scan because it must count the selected entries for
progress reporting.

The focused benchmark used the existing 256-record binary journal on an AMD
Ryzen 9 5950X with three samples per path:

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Legacy replay, before cache | 404,014; 462,104; 555,715 | 462,104 | 153,403 | 2,599 |
| Ordinary replay, after cache | 295,956; 330,922; 530,634 | 330,922 | 83,131 | 1,308 |
| Progress replay, after cache | 559,900; 565,184; 559,774 | 559,774 | 153,547 | 2,600 |

Compared with the before-cache median, ordinary replay is `1.40x` faster,
uses `1.84x` less allocated memory, and performs `1.99x` fewer allocations.
Progress replay remains on the original two-scan path and is included to show
that progress accounting was not traded away.

Verification uses:

```text
make test-journal-replay-fastpath
make race-journal-replay-fastpath
make benchmark-journal-replay-fastpath
make test-journal-replay
make benchmark-journal-replay
make test
```
