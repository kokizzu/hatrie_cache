# Dead-letter queue `DeadLettersInto`

`DeadLetterQueue.DeadLettersInto` copies retained failures into caller-owned
storage in the existing failure order. It is intended for monitoring, export,
and replay tooling that repeatedly inspects a bounded dead-letter queue.

```go
items := make([]DeadLetterItem[Job], 0, queue.DeadLetterLen())
for range polls {
    items = queue.DeadLettersInto(items)
    report(items)
}
```

The method clears and reuses the destination, grows it only when capacity is
insufficient, and keeps the existing shallow-copy ownership semantics. The
`Value` field is copied as a Go value exactly as it is by `DeadLetters()`;
callers that need deep value isolation must do that explicitly.

## Measurement

Workload: 4,096 retained integer dead letters, ten benchmark samples on an
AMD Ryzen 9 5950X.

| Operation | Median ns/op | B/op | Allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| `DeadLetters()` | 130,978 | 360,448 | 1 | 1.00x |
| Reused `DeadLettersInto()` | 6,487 | 0 | 0 | 20.19x faster |

The pre-change `DeadLetters()` median was 131,910 ns/op with the same memory
profile. The reusable API is opt-in; existing callers retain independent
returned-slice behavior.
