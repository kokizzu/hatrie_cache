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
| `hatrie_cache/hat/hatCodec` | Exact JSON encoded-size helpers used for request and response limits | Root JSON-size helpers delegate to it |
| `hatrie_cache/hat/hatHash` | Allocation-free FNV-64 and JSON-string hash variants | Root compatibility wrappers serve Bloom, Count-Min, Cuckoo, HyperLogLog, and XOR structures |
| `hatrie_cache/hat/hatStorage` | Storage backend and persistence-format identifiers, validation, compact reusable-index metadata, and generic tail trimming | Root storage pools and store lifecycles retain ownership; configuration aliases and vacancy tracking share the public model |
| `hatrie_cache/hat/hatTopology` | Cluster topology model, validation, fingerprinting, routing, and atomic JSON persistence | Root aliases its model; `TopologyStore` retains synchronization and its normalized routing fast path |
| `hatrie_cache/hat/hatMerkle` | Fixed 1,024-bucket mask selection and canonical inventory-mask wire encoding | Replication aliases `BucketMask`; mutable index/table ownership remains root-local |
| `hatrie_cache/hat/hatBackup` | Backup mode, manifest, file checksum, and partition-coverage model | Root aliases the portable model; creation and staged recovery retain storage ownership |
| `hatrie_cache/hat/hatDataStructure` | Standalone compact algorithms: Fenwick tree, Quantile Sketch, Roaring Bitmap, Sparse Bitset, HyperLogLog, Bloom Filter, and Cuckoo Filter shape calculation | Root keeps cache storage, generic JSON coercion, replication, and command adapters |
| `hatrie_cache/hat/hatSql` | SQL request/result wire model, HTTP client, resolver and observability contracts, UDF contracts, diagnostics, and the shared source-span lexer | Root compatibility aliases preserve existing callers; cache-backed planning, prepared templates, and execution retain cache ownership |

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
| SQL execution | `sql_query.go`, `sql_function.go`, and runtime adapters | Query planning, prepared templates, source resolution, and command-backed values share cache interfaces. The portable wire model, lexer, diagnostics, observer/resolver interfaces, and UDF contracts are in `hat/hatSql`. |
| Storage and recovery | `leveldb*.go`, `pebble*.go`, `snapshot*.go`, `journal*.go`, `backup*.go` | Their schemas and recovery guarantees are cross-validated together. |
| Replication | `replication*.go`, `election*.go`, `local_partition.go` | Ownership, topology, journal ordering, and transport safety are one protocol boundary. |
| Data structures | `*_filter.go`, `*_sketch.go`, `fenwick_tree.go`, `radix_tree.go`, `sparse_bitset.go`, `roaring_bitmap.go` | Each has command, snapshot, binary replication, storage, and compact-memory integrations. Fenwick, Quantile Sketch, Roaring Bitmap, Sparse Bitset, HyperLogLog, and Bloom Filter reusable algorithms are in `hat/hatDataStructure`; their cache adapters remain here. |

Fenwick tree, Quantile Sketch, Roaring Bitmap, and Sparse Bitset are completed data-structure
extractions. Their public types deep-copy snapshots and validate restored
state, while root adapters continue to own unchanged cache snapshot schemas,
binary replication, reusable storage, and `HatTrie` methods. Other structures
will follow this same algorithm-first rule rather than duplicating
implementations.

## Cache-Bound Structures

The remaining root data-structure files are intentionally not superficial
wrappers. They depend on cache-owned behavior that must not be duplicated in a
public component:

| Structure | Root-owned boundary | Extraction decision |
| --- | --- | --- |
| Count-Min Sketch | Generic JSON identity and exact command batches; the attempted state adapter made direct increments 1.14x slower. | Retain the mutable core locally; shared FNV hashing is already public. |
| Cuckoo Filter | Mutable buckets, deterministic relocation, fixed binary payloads, and generic JSON identity. | Keep the mutable core local; publish shape calculation and constants. |
| XOR Filter | Staged generic values and static build/retry state. | Keep building and lookup local; publish expected-item validation. |
| Top-K and reservoir sample | Cache cloning, generic JSON values, and durable snapshots. | Keep mutable heaps local; publish bounded-capacity validation. |
| Radix tree and collections | Cache clone/validation semantics and direct JSON serialization of arbitrary values. | Keep their value-bearing cores local rather than duplicate cache semantics. |

## Deliberately Retained Boundaries

Some apparent utility files remain root-local because moving them would either
duplicate protocol logic or alter a measured hot path:

| Area | Retained boundary | Reason |
| --- | --- | --- |
| Binary field codec | `binary_codec.go` | Snapshot, journal, persistent-store, and replication writers access its private backing buffer directly. A public migration requires replacing 143 accesses and must be benchmark-gated. |
| Replication framing | `replication_sync_wire.go`, `replication_outbox_binary.go` | Frames contain root-owned commands and use private packed-key arenas, pooled buffers, and lifecycle synchronization. |
| Command model and dispatch | `command*.go` | Requests carry root-owned `Map` and `Slice` value semantics, while execution couples validation, partition routing, cache mutation, journaling, and replication. |
| Backup execution and restore | `backup*.go` | Public manifest models are available in `hatBackup`; checkpoint creation and staged recovery must retain `HatTrie`, journal, and persistent-store lifecycle ownership. |
