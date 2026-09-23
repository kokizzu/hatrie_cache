# T231 After-Replace Audit Hooks

T231 adds an opt-in audit callback for successful `Space` mutations. Each
successful insert, replacement, or delete receives a monotonic nonzero
transaction ID from its `Space` instance and a copied old/new image.

## API

```go
space, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
	AfterReplace: func(audit hatDataStructure.SpaceReplaceAudit) {
		log.Printf("space mutation tx=%d key=%s delete=%t", audit.TransactionID, audit.Key, audit.Delete)
	},
})
```

`SpaceReplaceAudit` embeds `SpaceReplace`, so the callback can inspect
`TransactionID`, `Key`, `OldValue`, `NewValue`, `Exists`, and `Delete`.

## Semantics

- IDs start at `1` and increase monotonically for one `Space` instance.
- Each successful `Put` or existing-key `Delete` emits exactly one audit event.
- Invalid writes, `BeforeReplace` rejections, failed engine writes, and
  missing-key deletes do not emit an event. An engine failure after an ID is
  reserved can leave a gap.
- IDs are instance-scoped, not globally durable. A restored space starts a new
  instance sequence; applications needing cross-restart identity must add
  their own durable source identity.
- Old and new values are independent copies. Callback mutation cannot change
  stored state or caller-owned input.
- `AfterReplace` cannot reject an already accepted mutation. Use
  `BeforeReplace` for validation and conflict policy.
- The callback runs under the serialized space mutation lock. It must not call
  back into the same `Space` and should keep audit work bounded.
- When both hooks are configured, `OnReplace` runs first and `AfterReplace`
  runs second. Their event images are independent copies.

The zero value leaves `AfterReplace` disabled and does not allocate an audit
image or increment a transaction counter on the normal path.

## Cost

The focused memtx benchmark repeats a five-byte replacement. Values are
medians of five `-benchmem -cpu=1` samples on Linux/amd64 with an AMD Ryzen 9
5950X:

| Workload | CPU | Memory | Relative CPU |
| --- | ---: | ---: | ---: |
| Existing path before T231 | 50.04 ns/op | 8 B/op, 1 alloc/op | 1.00x |
| Hook disabled after T231 | 49.80 ns/op | 8 B/op, 1 alloc/op | 1.00x |
| Hook enabled, no-op callback | 106.7 ns/op | 24 B/op, 3 allocs/op | 2.14x |

The enabled path pays for copied event images, the transaction counter, and
the serialized callback. The default remains disabled and keeps the original
allocation profile. Raw samples are recorded in
[BENCHMARK.md](BENCHMARK.md#t231-after-replace-audit-hooks).
