# TT-030 Atomic Catalog DDL Batches

`hatSchema.SpaceCatalog.ApplyAtomic` provides an opt-in all-or-nothing batch
for named-space upserts and deletes. Each change is applied in order to a
private catalog map; the live map is replaced only after every definition,
change kind, and final space-count limit validates successfully.

```go
err := catalog.ApplyAtomic([]hatSchema.SpaceCatalogChange{
	{Kind: hatSchema.SpaceCatalogChangeUpsert, Definition: orders},
	{Kind: hatSchema.SpaceCatalogChangeDelete, Name: "users"},
})
```

Invalid batches leave the prior catalog unchanged. Deletes of missing names
are idempotent, and a batch may intentionally upsert/delete the same name in
order. Existing `Upsert` and `Delete` behavior is unchanged; callers opt in to
the transactional batch API.

This is the catalog-publication part of transactional DDL. It does not make
row mutations, physical index construction, persistence, or distributed
schema coordination transactional. Callers must still publish durable catalog
state and coordinate data-plane work around the atomic catalog boundary.

## Measurement

Five `-benchmem` samples were run on Linux/amd64, AMD Ryzen 9 5950X, over the
same eight prebuilt definitions and a final `List` snapshot:

| Path | Median ns/op | B/op | Allocs/op | Relative to sequential path |
| --- | ---: | ---: | ---: | --- |
| Existing sequential `Upsert` x8 | 3,324 | 3,784 | 31 | 1.00x |
| `ApplyAtomic` with eight upserts | 3,172 | 3,832 | 32 | 1.05x faster; 1.01x bytes; 1.03x allocations |

The atomic path has a small one-allocation/48-byte staging cost in this small
workload. Its value is publication correctness, not a throughput claim. The
default path remains unchanged and the benchmark keeps the cost visible for
future changes.

Raw samples (`ns/op`, `B/op`, `allocs/op`):

```text
sequential: 3418 3784 31
sequential: 3346 3784 31
sequential: 3187 3784 31
sequential: 3324 3784 31
sequential: 3214 3784 31
atomic:     3172 3832 32
atomic:     3227 3832 32
atomic:     3224 3832 32
atomic:     3153 3832 32
atomic:     3121 3832 32
```

Run it with:

```text
make benchmark-tt030-transactional-ddl
```
