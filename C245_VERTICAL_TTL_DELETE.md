# C245: Vertical TTL Deletion

Status: implemented as an opt-in `hatDataStructure.PersistentDeleteBitmap`
operation.

## Problem

A row TTL pass should not have to decode or touch wide payload columns just to
find expired rows. The deletion mask, primary key column, and narrow expiry
column are sufficient to identify rows for logical deletion. Physical
compaction remains a separate operation.

## API

`ApplyVerticalTTLDeletes(keys, expiryUnixNano, now, maxRows)` validates the
aligned inputs, scans only live bits in the persistent deletion bitmap, and
returns the expired row number plus key. A zero expiry means that the row has
no TTL. Matching rows are marked deleted after the scan; already deleted rows
are skipped.

`ApplyVerticalTTLDeletesInto(buffer, keys, expiryUnixNano, now, maxRows)` has
the same behavior but reuses a caller-owned candidate buffer. Reuse this form
in a repeated maintenance loop to avoid result allocations. `maxRows == 0`
means unlimited; a positive value bounds one pass. Invalid dimensions, a zero
timestamp, or a negative limit return an error before bitmap mutation.

The bitmap is not concurrency-safe by itself, matching the existing bitmap
contract. The owning table or part must provide synchronization. No existing
TTL scheduler or storage format changes, and no background work is enabled.

## Measurement

Five 200 ms samples on Linux amd64, AMD Ryzen 9 5950X, with 65,536 rows. Ten
percent of rows were expired, every seventeenth row was pre-deleted, and the
control carried a 512-byte payload. The control also produced the same row/key
candidate shape, but read each full row.

| Operation | Median time | Heap/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Vertical TTL, convenience result | 317,988 ns | 603,905 B | 11 | 6.78x faster than full-row control, but repeated slice growth costs heap |
| Vertical TTL, reusable buffer | 114,738 ns | 0 B | 0 | 18.78x faster than full-row control and no per-pass allocation |
| Full-row control | 2,155,322 ns | 163,840 B | 1 | Baseline; reads the 512-byte payload for every row |

The reusable form moves candidate-buffer memory from the timed operation to the
caller. It is the recommended form for recurring TTL maintenance. The simple
form remains useful for one-shot calls and preserves a small API surface.

Reproduce with `make benchmark-c245-vertical-ttl-delete`. Correctness,
race, and vet checks are available through the corresponding C245 Makefile
targets.
