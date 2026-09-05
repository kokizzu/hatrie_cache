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

Verification uses:

```text
make test-journal-replay-fastpath
make race-journal-replay-fastpath
make benchmark-journal-replay-fastpath
make test
```
