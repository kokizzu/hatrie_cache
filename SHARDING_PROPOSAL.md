# Sharding Proposal

This is a design proposal only. It keeps the current topology, election, and
replication model, but makes shard routing faster, more stable, and easier to
rebalance.

## Default Hash

Use XXH3 64-bit for key-to-slot hashing:

```text
tag = hash tag inside {...} when present and non-empty, otherwise full key
hash = XXH3 64-bit(tag, fixed_seed)
slot = hash & 65535
```

The fixed seed must be part of the persisted topology version so every node and
client computes the same route. XXH3 64-bit is the preferred default because it
is much faster than the current FNV bucket hash, has better avalanche behavior,
and is still deterministic across processes. It is not a security boundary; keep
auth and replication fingerprint checks for trust.

Use 65,536 logical slots as the default. The slot count is a power of two, so
routing is a cheap mask instead of modulo. It also gives enough granularity to
move small percentages of data during rebalancing without creating a huge
topology file.

Hash tags follow the Redis Cluster convention. `user:{42}:profile` and
`orders:{42}` route to the same slot because the non-empty text inside braces is
hashed. That gives clients a simple way to colocate related keys and batch
commands without introducing multi-key transactions.

## Topology Shape

Keep the existing `mode`, `bucket_count`, `bucket_ranges`, `shards`, and `nodes`
concepts, but set `bucket_count` to `65536` for new sharded topologies. Rename
the operator-facing language from "bucket" to "slot" in docs and CLI output over
time; internally they can remain compatible.

Each slot range maps to a shard. Each shard has:

- one primary node
- zero or more replica nodes
- an optional migration state

Migration states should be explicit:

- `active`: primary accepts reads and writes for the slot.
- `exporting`: old primary still accepts writes and streams data to the target.
- `importing`: target accepts replication and catch-up data but does not serve
  client writes yet.
- `cutover`: topology version has switched; clients should route to the target,
  and the old owner forwards or rejects with a route hint.

## Placement Planner

Use rendezvous hashing to build the desired slot ownership map when creating or
rebalancing a topology. For every slot, score each candidate node or virtual
node:

```text
score = XXH3 64-bit(topology_seed || slot_id || node_id || vnode_id)
owner = highest score
```

For weighted nodes, either give larger nodes more virtual nodes or use weighted
rendezvous scoring. Virtual nodes are easier to audit and produce deterministic
plans without floating point differences across languages.

Rendezvous hashing is the right planning algorithm because nodes can be added,
removed, or weighted without assuming contiguous node indexes. Only slots owned
by changed nodes need to move. The runtime hot path should not evaluate
rendezvous for every command; it should route through the persisted slot ranges.

## Rebalance Flow

1. Generate a target slot map with rendezvous hashing.
2. Diff current and target ownership into bounded migration batches.
3. Mark source slots `exporting` and target slots `importing` in a new topology
   version.
4. Stream source data to the target with `DUMP`/`INTERNALSET` or journal-stream
   replication, recording the source journal sequence used for the copy.
5. Catch up from that journal sequence until no gap remains.
6. Switch the slots to `cutover`, then `active`, and persist the new topology.
7. Clean source copies after replica acknowledgements or an operator-defined
   grace period.

Writes during migration should either stay on the old primary until cutover or
be synchronously forwarded to the target. The simpler first implementation is
old-primary writes plus journal catch-up; it has fewer split-brain cases.

## Client And Batch Behavior

Clients should cache the topology fingerprint and route by slot. On a
`not leader` or `wrong shard` response, clients refresh topology and retry if
the command is idempotent. Public `BATCH` requests should be grouped by slot on
the client when possible. The server can reject or split cross-slot batches
later, but the first phase can execute them in order through the existing command
path.

## Implementation Steps

1. Add a new hash mode to topology, for example `hash: "xxh3-64"` with
   `bucket_count: 65536`.
2. Keep the current FNV router for old topology files so existing clusters do
   not silently move keys.
3. Add deterministic tests for slot stability, hash tags, distribution, and
   old-topology compatibility.
4. Add a planner command that outputs slot moves but does not apply them.
5. Add migration states and read-only inspection endpoints before enabling
   automated movement.

The big win is predictable resharding: a fast hash on the hot path, explicit
slot ownership in the persisted topology, and rendezvous hashing only in the
offline planner where arbitrary node membership and weights matter.
