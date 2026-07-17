# Partitioning Proposal

This is a design proposal only. It does not change runtime behavior. The goal is
to support explicit operational partitions, especially multi-datacenter or
multi-region deployments, without inheriting the backup and migration complexity
of automatic sharding.

## Positioning

Partitioning should be the default distributed mental model for operators:

- partitions are explicit administrative ownership domains, such as region,
  datacenter, tenant group, or product boundary
- each partition has an authoritative primary region and optional replica
  regions
- backups, restores, audits, and disaster recovery happen per partition
- moving a partition is an operator-approved event, not automatic background
  balancing

Sharding remains useful for a single logical dataset that is too large for one
node, but it should be opt-in and mostly automatic before being recommended.
Automatic sharding needs planner automation, route hints, migration state,
rollback, and backup-aware tooling. Until that exists, explicit partitioning is
safer for production because ownership is obvious and backup boundaries remain
small and predictable.

Any future automatic sharding mode should default to off. A node with no
partition or sharding configuration should keep the current single-node behavior.

## Partition Model

Add a future topology mode named `partitioned`:

```json
{
  "version": 1,
  "mode": "partitioned",
  "self": "sg-a",
  "default_partition": "sg",
  "nodes": [
    {"id": "sg-a", "address": "https://sg-a.example.internal", "region": "sg"},
    {"id": "us-a", "address": "https://us-a.example.internal", "region": "us"}
  ],
  "partitions": [
    {
      "id": "sg",
      "primary": "sg-a",
      "replicas": ["us-a"],
      "match": {"prefixes": ["sg:", "apac:"], "tags": {"region": "sg"}}
    },
    {
      "id": "us",
      "primary": "us-a",
      "replicas": ["sg-a"],
      "match": {"prefixes": ["us:"], "tags": {"region": "us"}}
    }
  ]
}
```

A partition is not a hash bucket. It is an explicit rule that maps a key or
request to an operational owner. For the first implementation, use prefix rules
because they are easy to inspect, easy to back up, and easy to explain during an
incident.

Recommended key convention:

```text
<partition>:<tenant-or-domain>:<entity>
sg:tenant-42:session:abc
us:tenant-99:profile:123
```

Optional future matchers can include HTTP/gRPC metadata, tenant IDs, or explicit
partition fields in command requests. Prefix matching should remain the
portable baseline because it works with existing commands and backup tools.

## Write Ownership

Each partition has one write primary at a time. Mutating commands for a partition
must be accepted only by that partition primary unless the operator intentionally
enables write forwarding.

Recommended first behavior:

- local partition primary accepts reads and writes
- local partition replica accepts reads only when explicitly allowed
- non-owner returns a route hint with the partition id and owner address
- cross-partition `BATCH` is rejected for mutating commands unless every command
  is independently safe to retry

Route hints should be partition-oriented, not slot-oriented:

```text
PARTITION partition_id host:port topology_epoch
```

For example:

```text
PARTITION sg sg-a.example.internal:8080 12
```

Clients can then retry against the partition primary and refresh topology when
the epoch changes.

## Replication

Use journal replication as the primary mechanism. Command fanout can still exist
for low-latency local replicas, but the reliable baseline should be ordered
journal pull per partition.

Each journal entry should eventually carry partition metadata:

```json
{
  "sequence": 9918273,
  "partition": "sg",
  "command": "SET",
  "key": "sg:tenant-42:session:abc"
}
```

Partition replicas pull only the partitions they are assigned to. This avoids
shipping unrelated regional data across datacenters, reduces restore blast
radius, and lets operators pause one partition without stalling the whole
cluster.

## Backup And Restore

Partitioning should make backup simpler than sharding:

- every backup artifact records one or more partition ids
- a full-node backup is valid only for the partitions owned or replicated by the
  node at the backup time
- restore can target one partition without rewriting unrelated keys
- restore rehearsal should validate partition id, topology epoch, journal
  sequence, and expected primary before applying data

Recommended backup layout:

```text
backup/
  sg/
    manifest.json
    snapshot.hc
    commands.journal
    cache.leveldb/
  us/
    manifest.json
    snapshot.hc
    commands.journal
    cache.leveldb/
```

The manifest should include:

- partition id
- topology fingerprint and topology epoch
- primary node at backup time
- included key prefixes or matcher rules
- snapshot format and journal format
- snapshot journal sequence
- LevelDB format and compaction metadata
- checksum for every artifact

Partition restore should be explicit:

```text
make restore PARTITION=sg BACKUP_DIR=backup/sg
```

If `PARTITION` is not set, restore should refuse partitioned backups unless the
operator passes an explicit all-partitions flag.

## Multi-Datacenter Behavior

A partition's primary should normally live in the region closest to its writes.
Examples:

- `sg` partition writes are primary in Singapore and replicated to the US
- `us` partition writes are primary in the US and replicated to Singapore
- global read replicas can serve stale reads only when the client opts in

Failover should be per partition. If Singapore is down, only partitions whose
primary was in Singapore should fail over. US-owned partitions should continue
without topology churn.

The failover state should be explicit:

- `active`: current primary accepts writes
- `read_only`: partition serves reads but rejects writes
- `failing_over`: candidate primary is catching up from journal
- `promoted`: new primary accepts writes after sequence fence validation
- `recovering`: old primary rejoins as a replica

Promotion must require a journal sequence fence. The new primary should not
accept writes until it proves it has replayed through the old primary's accepted
sequence, or the operator accepts a documented data-loss boundary.

## Operations

Initial commands and endpoints should be inspection-first:

- show partition map
- route key to partition
- list partitions hosted by this node
- report partition replication lag
- run partition backup
- run partition restore rehearsal
- mark partition read-only
- promote partition replica after fence validation

Do not add automatic partition movement in the first implementation. Movement
between regions should produce a dry-run plan that includes:

- source and target region
- affected prefixes or matchers
- estimated key count and bytes
- backup requirement before movement
- journal fence at copy start
- catch-up progress
- rollback point

## Relationship To Sharding

Partitioning and sharding solve different problems:

| Property | Partitioning | Sharding |
| --- | --- | --- |
| Split method | Explicit region/tenant/domain rule | Hash slot or bucket |
| Best for | Multi-datacenter ownership, DR, backup boundaries | Scaling one hot dataset |
| Movement | Manual, operator-approved | Eventually automatic planner |
| Backup | Per partition, easy to reason about | Needs slot-aware backup metadata |
| Client contract | Key prefix or partition field | Hash tag / slot routing |
| Failure domain | Partition or region | Slot range / shard |

The recommended order is:

1. Keep no distributed topology by default for single-node deployments.
2. Use `full_replica` when every node should own every key.
3. Use `partitioned` when data has natural ownership boundaries.
4. Use automatic sharding only after planner, migration, backup, and rollback
   tooling are mature enough that operators do not manually juggle slots.

## Implementation Steps

1. Add `mode: "partitioned"` to topology validation behind explicit config.
2. Add `partitions` metadata without changing command routing.
3. Add read-only inspection APIs for partition map and key routing.
4. Add prefix-based route checks for mutating commands when
   `ENFORCE_LEADER_WRITES=true`.
5. Add partition id to journal metadata while preserving compatibility with old
   journals.
6. Add partition-aware journal pull filters.
7. Add partition backup manifest and restore rehearsal.
8. Add per-partition failover state and sequence-fence validation.
9. Add route hints and client retry support.
10. Only then consider automatic movement or sharding integration.

The big win is operational clarity: region-owned data can be backed up,
restored, failed over, and audited independently. In this plan, automatic
sharding stays off by default until the automation is strong enough to manage
its added complexity.
