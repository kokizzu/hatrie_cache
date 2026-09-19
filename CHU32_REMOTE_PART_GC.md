# CH-U32 Remote-Part Garbage Collection

CH-U32 adds an opt-in, importable reachability planner and explicit delete
executor for remote parts. It does not own credentials, remote listing, or
manifest storage. The caller supplies the live references read from its
manifests and the object listing returned by its object-store adapter.

## Plan and apply

```go
plan, err := hatStorage.PlanRemotePartGarbageCollection(
    liveReferences,
    listedObjects,
    hatStorage.RemotePartGCOptions{
        MinAge: 24 * time.Hour,
        Now:    time.Now().UTC(),
    },
)
if err != nil {
    return err
}

// Persist or review plan before invoking the destructive operation.
result, err := hatStorage.ExecuteRemotePartGarbageCollection(ctx, store, plan)
```

`RemotePartGCObject.LastModified` is required. The default retention is 24
hours, which protects an object from a stale or eventually consistent
manifest/listing view. `MaxCandidates` defaults to 10,000 per plan and cannot
exceed the hard limit of 1,000,000. A zero `Now` uses the current UTC time;
tests and operators can provide a fixed value for deterministic plans.

The planner:

- validates remote URIs through the existing remote-part reference policy;
- collapses identical duplicate listings and rejects conflicting metadata;
- marks every URI in the supplied live manifest references as reachable;
- selects only unreachable objects older than the retention window;
- sorts candidates by canonical URI for stable review and application; and
- returns reclaimable bytes without performing network or filesystem I/O.

The executor validates the complete plan before the first delete, deletes only
the listed candidates in URI order, honors cancellation between deletes, and
returns completed object/byte counts plus the failed URI. It does not retry,
discover new objects, or automatically abort/roll back already completed
deletes. Remote listing, manifest consistency, authorization, retry policy,
orphan-upload retention, and audit logging remain caller/operator concerns.

## Safety boundaries

This API is deliberately not a background goroutine and has no default runtime
cost. A caller must explicitly create and authorize a plan before invoking the
destructive executor. Invalid timestamps, unsafe URIs, duplicate conflicts,
candidate-limit violations, mutated plans, canceled contexts, and store errors
are covered by focused tests. A canceled or invalid plan makes zero delete
calls.

## Measurements

Five `-benchmem` samples ran on Linux/amd64 with an AMD Ryzen 9 5950X. The
fixture contains 10,000 listed objects, 2,000 reachable references, and 8,000
old unreachable candidates. The delete adapter is a no-op, so network latency
and remote bandwidth are intentionally excluded. Raw samples are in execution
order; medians are the middle sorted sample.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Naive O(n x m) reachability scan, no retained plan | 62,690,218; 60,933,069; 61,822,760; 60,714,458; 63,467,288 | 61,822,760 | 298 | 12 | 1.00x |
| Naive O(n x m) plan, retains candidates | 54,470,632; 55,929,603; 57,292,393; 55,508,608; 56,729,108 | 55,929,603 | 483,601 | 12 | 1.00x |
| CH-U32 hash reachability plus in-place sorted plan | 1,655,524; 1,742,548; 1,649,398; 1,703,955; 1,740,435 | 1,703,955 | 674,608 | 13 | 0.03x; 32.82x faster than fair naive plan |
| CH-U32 explicit delete executor | 718,378; 736,666; 720,331; 768,033; 728,822 | 728,822 | 3 | 0 | allocation-free |

The optimized planner uses about 1.40x the fair naive plan's memory because it
retains canonical candidate metadata and performs deterministic sorting, while
cutting CPU by about 32.8x. The plan's memory is bounded by the listed object
count and candidate limit rather than by an unbounded remote listing.
