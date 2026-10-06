# Durable Cluster Membership

`hat/hatTopology` now provides an opt-in durable membership registry for
cluster control-plane state. It records active members and removed-member
tombstones, assigns a monotonically increasing generation to each name, and
persists a versioned, checksummed image.

This is a durable membership record, not a consensus implementation. The
caller still owns leader election, quorum decisions, replication, transport,
authentication, and deciding when a membership change is safe to publish.

## Usage

```go
store, err := hatTopology.NewFileMembershipStore("/var/lib/hatrie/membership.hmb")
if err != nil {
	return err
}

registry, err := hatTopology.NewDurableMembershipRegistry(
	store,
	hatTopology.DurableMembershipOptions{},
)
if err != nil {
	return err
}

member, err := registry.Join("node-a", "10.0.0.10:7400")
if err != nil {
	return err
}

// Use the generation observed at join time to fence stale leave requests.
if err := registry.Leave(member.Name, member.Generation); err != nil {
	return err
}
```

The zero-value options use `DefaultDurableMembershipMaxMembers` (256). A
custom limit can be supplied with `DurableMembershipOptions{MaxMembers: N}`;
the implementation rejects values above `MaxDurableMembershipMembers` (4096).
Names, addresses, image size, and member count are bounded before they are
stored or decoded.

## Semantics

- `Join` normalizes and validates the name and address, rejects duplicate
  active names or addresses, and persists the change before returning it.
- A removed name remains as a tombstone. Rejoining the same name increments
  its generation, so an old leave or update cannot silently affect the new
  member.
- `Leave(name, generation)` requires the current active generation. A stale
  generation returns `ErrDurableMembershipStaleGeneration`.
- `Snapshot` returns members in deterministic name order and does not expose
  the registry's internal slice.
- A failed store save rolls the in-memory change back.
- A missing membership file is treated as an empty registry. A malformed,
  oversized, or checksum-invalid file fails closed with
  `ErrDurableMembershipCorrupt`; it is not silently replaced.

## File Format And Recovery

The file store writes an HMB1 image containing a format version, revision,
member records, and CRC32C checksum. It writes a mode `0600` temporary file,
flushes the file, renames it into place, and syncs the parent directory. The
registry revision increases on every successful join or leave.

Back up the membership file together with the data snapshot that it describes.
Restore it atomically before starting a node. If the restored image is from an
older point in time, the caller must apply its normal cluster recovery and
quorum rules; this component does not resolve divergent histories.

Keep the file path writable only by the service account. The format contains
member names and addresses, but no credentials or transport secrets.

## Benchmark

The benchmark uses the same five-run, 200 ms configuration for the existing
map lookup baseline and the durable registry lookup. The registry lookup is a
control-plane operation; file I/O is intentionally not on the read path.

| Operation | Median | Memory | Relative to baseline |
| --- | ---: | ---: | ---: |
| Existing map lookup | 9.138 ns/op | 0 B/op, 0 allocs/op | 1.00x |
| Durable registry lookup | 23.28 ns/op | 0 B/op, 0 allocs/op | 0.39x |
| Existing map lookup, parallel | 41.97 ns/op | 0 B/op, 0 allocs/op | 1.00x |
| Durable registry lookup, parallel | 33.38 ns/op | 0 B/op, 0 allocs/op | 1.26x |
| Durable snapshot encoding | 2,466 ns/op | 3,464 B/op, 11 allocs/op | write-path only |

The sequential lookup is slower than the deliberately minimal map baseline,
while the read-only parallel benchmark was faster in this run. The additional
validation, generation semantics, bounded snapshots, and persistence contract
are intended for membership control traffic, not for per-request data-path
lookups.

The reproducible benchmark target is:

```text
make codex-tu13-bench
```

It runs in the isolated feature worktree used during development and uses
`-benchtime=200ms -count=5`.
