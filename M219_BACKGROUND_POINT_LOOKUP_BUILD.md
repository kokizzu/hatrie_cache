# M219: Background Point Lookup Builds

## Decision

M219 adds an opt-in asynchronous build path for maintained materialized-view
point lookup indexes. The existing `CreatePointLookupIndex` API remains
synchronous and unchanged by default.

The design follows the useful part of Materialize-style frontier reporting:
the builder exposes the amount of an immutable snapshot that has been
processed, but the public index is not visible until the complete snapshot is
ready. A refresh or drop that invalidates the captured snapshot prevents
publication instead of exposing stale or partial rows.

## API

```go
build, err := views.StartPointLookupIndexBuild(ctx, hatSql.MaterializedViewPointLookupDefinition{
    Name:     "people_by_id",
    ViewName: "people_view",
    Key: func(row hatSql.Row) (string, error) {
        return fmt.Sprint(row["id"]), nil
    },
})
if err != nil {
    return err
}

status := build.Status()
status, err = build.Wait(ctx)
```

`Status()` and `Wait()` return `MaterializedViewPointLookupBuildStatus` with:

- `State`: `building`, `ready`, `failed`, or `canceled`.
- `Frontier`: rows fully incorporated into the private build.
- `TotalRows`: rows in the captured view snapshot.
- `ViewRevision`: the snapshot revision used by the build.
- `Err`: a terminal error string when applicable.

`Cancel()` requests cooperative cancellation. A key function that is already
running must return before cancellation can be observed. A build is keyed by
index name; duplicate active builds are rejected. A build that finishes after
the source view has refreshed fails with
`ErrMaterializedViewPointLookupBuildStale`, and no index is published.

Progress updates are batched every 64 rows and always include the final row.
This keeps status observation useful without adding a mutex operation to every
row in the build loop.

## Measurement

Workload: 10,000 rows, integer point key, AMD Ryzen 9 5950X, twenty iterations
per sample and five samples. The synchronous row-build path is the baseline.
The `background_start` row measures only the foreground request/return path;
its worker is gated so the full index build does not contaminate that latency
measurement. A worker can still be scheduled before the timer stops, so its
`B/op` column is only an incidental start-path sample; `background_total`
includes the complete asynchronous build and wait.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Synchronous publish | 1,788,547 | 906,811 | 19,936 | baseline |
| Background start only | 15,173 | 10,732 | 7 | 117.9x shorter foreground return |
| Background start plus wait | 1,740,273 | 907,876 | 19,944 | 0.97x total time, 0.1% more bytes, 0.04% more allocations |

Raw samples:

```text
synchronous_publish ns/op: 1789573 1766003 1817593 1579010 1788547
background_start ns/op:     13830   15538   15173   34728   13440
background_total ns/op:   1928945 1740273 1632067 1800618 1422406
```

The result is primarily a foreground-latency improvement, not a guaranteed
reduction in total indexing work. The measured total work was approximately
equal to synchronous publication within benchmark variance. Callers that need
to keep readers and refreshes responsive can use the asynchronous API; callers
that prefer a single blocking operation can continue using the synchronous
API.

## Verification

```text
make test-m219-background-point-lookup
make test-m219-related-materialized
make race-m219-background-point-lookup
make benchmark-m219-background-point-lookup
```

The tests cover observable building state, zero partial reads, atomic ready
publication, stale refresh rejection, cancellation, duplicate-build
protection, and successful point lookup after completion.
