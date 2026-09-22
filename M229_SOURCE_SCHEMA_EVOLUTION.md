# M229: Additive Source Schema Evolution

## Purpose

The existing `SpaceChangefeed` string schema version remains the default and
keeps its exact-match behavior. Opt-in typed schemas add a compatibility gate
for rolling producer and consumer upgrades.

```go
v1 := hatReplication.ChangefeedSchema{
    Version: "orders-v1",
    Fields: []hatReplication.ChangefeedSchemaField{
        {Name: "id", Type: "int64"},
        {Name: "status", Type: "string", Nullable: true},
    },
}
feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{
    Space: "orders",
    Schema: &v1,
})
if err != nil {
    return err
}
_, err = feed.EvolveSchema(hatReplication.ChangefeedSchema{
    Version: "orders-v2",
    Fields: []hatReplication.ChangefeedSchemaField{
        {Name: "id", Type: "int64"},
        {Name: "status", Type: "string", Nullable: true},
        {Name: "created_at", Type: "timestamp", Nullable: true},
    },
})
```

Consumers opt in with `SpaceChangefeedSubscribeOptions.ExpectedSchema`. An
older consumer can accept a newer producer when the producer only adds fields;
unknown producer fields are ignored. A newer consumer can read an older
producer only when its missing fields are nullable or have defaults.

## Compatibility rules

| Change | Result |
| --- | --- |
| Add nullable field | Accepted |
| Add field with a default | Accepted |
| Remove an existing field | Rejected |
| Change an existing field type or nullability/default contract | Rejected |
| Add a required field without a default | Rejected |
| Reuse a schema version with different fields | Rejected as a version conflict |

Schema names, types, versions, field counts, duplicate names, invalid UTF-8,
NUL bytes, and lengths are bounded and validated before a schema is installed.
`EvolveSchema` validates and publishes the new descriptor atomically. A legacy
subscriber using `ExpectedSchemaVersion` is fenced when the feed moves to a
different version; a typed subscriber that passed the additive check remains
connected.

`ChangefeedSchemaRegistry` exposes the same atomic evolution and compatibility
checks for producers or consumers that do not use `SpaceChangefeed`.

## Measurements

Five samples per benchmark, Linux amd64, AMD Ryzen 9 5950X. The schema check
workload used 256 existing fields and eight additive fields.

| Operation | Final result | Allocation cost |
| --- | ---: | ---: |
| 256-to-264 field evolution check | 145.5 us/op | 114.7 KB/op, 15 allocs/op |
| Producer/consumer compatibility check | 95.0 us/op | 81.8 KB/op, 11 allocs/op |
| Legacy exact-version publish | 224.6 ns/op | 16 B/op, 2 allocs/op |
| Typed additive-schema publish | 218.7 ns/op | 16 B/op, 2 allocs/op |

The first implementation checked typed compatibility on every event and measured
223.5 ns/op versus 190.5 ns/op for legacy publish, about 17% slower with the
same allocations. It was removed before commit: compatibility is validated at
subscribe/evolve time, so the final publish path has no per-event schema-map
allocation or typed-check overhead. The control-plane maps are intentionally
bounded and are not used in the event hot path.

## Verification

```text
make test-m229-source-schema
make benchmark-m229-source-schema
make race-m229-source-schema
make vet-m229-source-schema
make test-m229-package
```
