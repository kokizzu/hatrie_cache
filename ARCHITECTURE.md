# Architecture

`hatriecache` remains the cache-server package. It owns the C HAT-trie
binding, command execution, snapshots, persistence, replication, monitoring,
and SQL because those subsystems share cache values, locking, and durable wire
formats.

Independent components are published under `hat/hat[Name]` so applications can
import them without starting the cache server:

| Package | Responsibility | Root compatibility API |
| --- | --- | --- |
| `hatrie_cache/hat/hatAuth` | Constant-time current/previous token validation and bearer parsing | Internal server call sites import it directly |
| `hatrie_cache/hat/hatHttp` | HTTP gzip negotiation, `Vary` handling, and decoded request limits | Monitoring and command wire import it directly |
| `hatrie_cache/hat/hatRate` | Bounded sharded token-bucket limiting | `hatriecache.RateLimiter` and `NewRateLimiter` aliases |
| `hatrie_cache/hat/hatAudit` | Concurrent JSONL audit logging and recent-event retention | `AuditEvent`, `AuditLogger`, and constructors alias it |
| `hatrie_cache/hat/hatMetrics` | Atomic API audit, write-protection, and rate-limit counters | `APIMetrics`, `APIMetricsSnapshot`, and constructor aliases |

## Importing a component

```go
import (
    "time"

    "hatrie_cache/hat/hatRate"
)

limiter := hatRate.NewRateLimiter(100, time.Second)
if limiter.Allow("client-42") {
    // Handle the request.
}
```

## Extraction Rule

New public packages must not import the root `hatriecache` package. This keeps
the dependency graph one-way: the cache server may use a component, while a
component remains usable on its own. The package must have direct-import tests
and the root compatibility surface must remain covered by the full test suite.

## Core Migration Boundaries

The root still contains grouped domain files rather than arbitrary utilities:

| Domain | Root files | Why it remains together |
| --- | --- | --- |
| SQL | `sql*.go` | Query planning, source resolution, and command-backed values share cache interfaces. |
| Storage and recovery | `leveldb*.go`, `pebble*.go`, `snapshot*.go`, `journal*.go`, `backup*.go` | Their schemas and recovery guarantees are cross-validated together. |
| Replication | `replication*.go`, `election*.go`, `local_partition.go` | Ownership, topology, journal ordering, and transport safety are one protocol boundary. |
| Data structures | `*_filter.go`, `*_sketch.go`, `fenwick_tree.go`, `radix_tree.go`, `sparse_bitset.go`, `roaring_bitmap.go` | Each has command, snapshot, binary replication, storage, and compact-memory integrations. |

Fenwick tree is the first planned data-structure extraction candidate. Before
moving it, its compact snapshot, binary replication, reusable storage, and
`HatTrie` methods need a stable public data model; copying the implementation
would create divergent correctness and wire-format behavior.
