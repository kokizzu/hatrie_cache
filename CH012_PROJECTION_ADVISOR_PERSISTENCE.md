# CH-012 Projection Advisor Persistence

This is the durable-history slice of the ClickHouse-style workload-driven
projection advisor. It is explicitly opt-in: query execution, planner
selection, projection creation, and refresh scheduling remain unchanged unless
the caller invokes persistence.

## API

```go
store, err := hatSql.NewFileSQLProjectionAdvisorStore(
    "/var/lib/hatrie/projection-advisor.spa",
)
if err != nil {
    return err
}

if err := advisor.RestoreFrom(ctx, store); err != nil {
    return err
}

// After collecting a bounded batch of feedback:
if err := advisor.Persist(ctx, store); err != nil {
    return err
}
```

`SQLProjectionAdvisorStore` is the importable interface for alternate stores.
`FileSQLProjectionAdvisorStore` is the built-in implementation. `Restore`
validates the complete recommendation set and swaps it in only after every
entry passes validation, so a failed restore cannot partially replace live
history.

## Snapshot Safety

The file format is a bounded `SPA1` frame containing a versioned JSON payload
and a CRC32 checksum. It stores query IDs, source dependencies, referenced
field shape, slow-query counts, and total elapsed nanoseconds; it never stores
SQL text, literal values, or result rows.

- Maximum payload: 8 MiB.
- Maximum recommendations: 65,536.
- Writes use a temporary owner-only (`0600`) file, `fsync`, atomic rename, and
  parent-directory sync.
- Missing files restore as an empty advisor.
- Truncation, checksum failure, unsupported versions, unsafe permissions,
  symlinks, malformed entries, and duplicate keys are rejected.
- The parent directory must already exist; startup does not create directories
  implicitly.

## Measured Cost

Command: `make benchmark-ch012-projection-advisor-persistence`

The codec benchmark uses 32 recommendations on Linux amd64 with an AMD Ryzen
9 5950X and `-benchtime=200ms`:

| Path | ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Encode | 51,042 | 41,575 | 583 | 1.00x |
| Decode | 133,748 | 29,928 | 1,006 | 2.62x encode |

The resulting SPA1 frame is 8,284 bytes for that fixture. The benchmark
measures memory encoding/decoding only; filesystem write, `fsync`, and rename
latency depend on the storage device and are intentionally outside the codec
comparison. This cost is paid only when `Persist` or `RestoreFrom` is called,
not on the advisor's feedback or recommendation hot path.

## Verification

```text
make test-ch012-projection-advisor-persistence
make race-ch012-projection-advisor-persistence
make vet-ch012-projection-advisor-persistence
make measure-ch012-projection-advisor-persistence
```

The focused tests cover round-trip reconstruction, missing-file cold start,
corruption detection, capacity and duplicate rejection, transactional restore
failure, and rejection of broad file permissions.
