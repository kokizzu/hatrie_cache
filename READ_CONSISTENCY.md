# Read Consistency

`hatReplication` supports explicit freshness policies when choosing a read
replica. The existing `SelectReadReplica` API remains the compatibility
entry point and keeps its previous read-after-write behavior.

## Levels

| Level | Eligibility rule | Use when |
| --- | --- | --- |
| `eventual` | Any candidate is eligible; the normal deterministic preference still applies. | Lowest read latency is more important than freshness. |
| `bounded-staleness` | Replica lag from `ObservedFrontier` must be at most `MaxLag`. | A bounded freshness window is acceptable. |
| `read-after-write` | Replica frontier must reach `RequiredFrontier` and lag must be at most `MaxLag`. | A session must observe acknowledged writes. |

Selection remains deterministic within the eligible set: highest frontier,
then highest health score, then lexical node name. Candidate names are trimmed
and blank names are rejected. No input slice is mutated.

## Go API

```go
level, err := hatReplication.ParseReadConsistencyLevel("bounded-staleness")
if err != nil {
	return err
}

replica, err := hatReplication.SelectReadReplicaWithConsistency(
	candidates,
	hatReplication.ReadReplicaPolicy{
		ObservedFrontier: observed,
		RequiredFrontier: sessionFrontier,
		MaxLag:           2,
	},
	level,
)
```

Accepted parser aliases are `bounded` for `bounded-staleness` and `session`
for `read-after-write`. An empty configuration value defaults to
`read-after-write`, preserving the old selector contract. Unknown values return
`ErrReadConsistencyInvalid`; an explicit level with no eligible candidate
returns `ErrNoEligibleReadReplica`.

`eventual` deliberately ignores `RequiredFrontier` and `MaxLag`. It still
uses the same candidate validation and deterministic ordering as the legacy
selector.

## Operational Guidance

Set `RequiredFrontier` from the caller's acknowledged write/session position,
and set `ObservedFrontier` from the replica-health report used by the caller.
Use a non-zero `MaxLag` only when bounded staleness is an intentional product
policy. If no candidate satisfies the selected level, retry after refreshing
replica progress or route the read to the authoritative node.
