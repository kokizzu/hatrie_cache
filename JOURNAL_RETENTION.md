# Journal Retention

The command journal can rotate into immutable files under
`<JOURNAL_PATH>.segments/`. Retention is evaluated after rotation and when a
segmented journal is opened.

## Defaults

The existing count policy remains the default:

```text
journal-segment-max-bytes = 67108864
journal-retained-segments = 16
journal-retained-bytes   = 0
```

`journal-retained-bytes=0` disables byte-budget accounting. Existing callers
that do not set the new field keep the previous count-only behavior.

## Configuration

CLI:

```text
./hatrie-cache \
  -journal-path data/commands.journal \
  -journal-segment-max-bytes 67108864 \
  -journal-retained-segments 16 \
  -journal-retained-bytes 1073741824
```

Makefile wrapper:

```text
make monitoring-server \
  JOURNAL_PATH=data/commands.journal \
  JOURNAL_RETAINED_BYTES=1073741824
```

The JSON config key is `journal_retained_bytes`. Embedded callers can set
`CommandJournalOptions.RetainedBytes` directly.

## Semantics

- The byte budget applies to closed archived segments only. The active file is
  bounded separately by `journal-segment-max-bytes`.
- Count and byte limits are applied together. The oldest complete segments are
  removed until both limits are satisfied.
- Deletion is whole-file and oldest-first; a segment is never truncated.
- At least the newest archived segment is retained. If that one segment is
  larger than the configured budget, the budget cannot be met until later
  rotation creates a newer boundary.
- Segments needed by unacknowledged replication outbox records or SQL
  projection watermarks remain protected, so the configured limits are a
  target rather than a permission to break recovery.
- Without segmentation (`journal-segment-max-bytes=0`), there are no archived
  segments for this policy to prune.

Choose a budget that covers the longest expected replica outage and projection
rebuild lag. A follower that requests a sequence older than the retained
boundary receives the existing compacted-journal response and must use the
configured snapshot/full-sync fallback.

## Backup And Recovery

The active journal and its `.segments` directory are one recovery set. Copy
both together, or use the server-side atomic backup bundle. A byte budget can
delete journal history between backups, so verify the snapshot/checkpoint
sequence is newer than the oldest history needed by every replica before
reducing the budget.

The policy is local and deterministic. It does not change journal records,
wire formats, replay semantics, or command ordering. It only changes which
old complete segment files remain available for incremental catch-up.

## Cost

With the byte budget disabled, pruning retains the existing count-only path.
When enabled, each prune check stats archived files to calculate their total
size. This adds maintenance CPU, filesystem calls, and temporary allocations;
it is not part of every command’s hot path. See the raw measurements in
[BENCHMARK.md](BENCHMARK.md#journal-byte-budget-retention).
