# Dead-Letter Queue Metrics

`DeadLetterQueue.Stats(now)` returns an allocation-free operational snapshot
without copying pending values or dead-letter payloads.

The snapshot includes:

- pending depth and backing capacity;
- whether the earliest pending item is ready, its deadline, and consumer lag;
- retained dead-letter depth and configured limit;
- oldest failure age and maximum recorded retry attempt.

`Ready` is intentionally a boolean for an O(1) pending-queue read. It reports
whether the earliest heap item is ready; it does not count every ready item.
Callers must synchronize access when a queue is shared between goroutines.

## Benchmark

The focused benchmark uses 4,096 pending items and 128 retained dead letters.
Each result is the median of five runs on the same host:

| Scrape path | ns/op | B/op | allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Manual `DeadLetters()` copy and scan | 2,053 | 12,288 | 1 | 1.00x |
| `Stats(now)` | 615.4 | 0 | 0 | 3.34x faster |

The snapshot avoids the per-scrape dead-letter slice allocation and keeps the
pending inspection to the heap root. It is a metrics API, not a replacement
for an exact ready-item count.
