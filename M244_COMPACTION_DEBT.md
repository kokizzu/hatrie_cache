# M244 Compaction Debt At The Logical Frontier

M244 adds an explicit compaction-debt metric to
`TypedTableAggregateArrangementStats`. The metric follows the existing
frontier-retention convention and makes the retained-history distance visible
without requiring callers to subtract fields themselves.

## Definition

For each arrangement stats report:

```text
CompactionDebt = max(0, SourceSequence - CompactedThrough)
```

`SourceSequence` is the table's current logical frontier. `CompactedThrough`
is the oldest retained changefeed frontier. The metric counts sequence units
of retained history, not bytes and not physical allocator usage. It is
independent of `Checkpoint`, which describes how far that arrangement has
applied changes; arrangement lag remains visible through the existing
`Checkpoint` and `SourceSequence` fields.

The value is zero at an empty or fully compacted table and never underflows if
an invalid or future frontier is observed. The existing logical and physical
compaction operations are unchanged.

```go
stats, err := arrangement.Stats()
if err != nil {
    return err
}
fmt.Println(stats.CompactionDebt)
```

The JSON field is `compaction_debt`. No write-path state, background worker,
or configuration flag is added.

## Measurement

The benchmark calls `TypedTableAggregateArrangement.Stats()` on a 256-row,
32-group arrangement. It runs the exact same benchmark file in the M243
parent worktree and in the M244 worktree, with five `-benchmem` samples on
Linux amd64, AMD Ryzen 9 5950X.

| Version | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| M243 parent | 1,311 | 0 | 0 |
| M244 current | 1,331 | 0 | 0 |

Raw samples:

```text
parent: 1311 1297 1312 1308 1353 ns/op; 0 B/op; 0 allocs/op
current: 1394 1300 1370 1331 1212 ns/op; 0 B/op; 0 allocs/op
```

The current median is 20 ns/op higher, about 1.5% slower, with the same zero
allocation profile. The metric is computed while assembling the existing stats
value and does not add work to normal table reads, writes, or compaction. This
bounded cost on an explicit diagnostic call is accepted for the new operational
visibility.

## Verification

The regression test verifies debt at the live frontier, after partial logical
compaction, after full compaction, and in JSON. Run:

```text
make test-m244
make benchmark-m244
make race-m244
make vet-m244
```

The focused M244 test, race test, and vet pass. The broader `make
test-m244-package` check currently reports the existing checkpoint-recovery
failures `TestTypedTableAggregateArrangementCheckpointRestoresGlobalAggregate`
and `TestTypedTableAggregateArrangementCheckpointRestoresWithoutReplay`; the
new compaction-debt tests are not involved in those failures.
