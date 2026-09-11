# Selective Snapshot Backups

Selective backup scope is available through `BackupBundleOptions.KeyPrefixes`.
It creates a normal, checksummed snapshot bundle containing every key whose
raw key bytes start with at least one configured prefix.

```go
manifest, err := hatCache.CreateBackupBundle("/var/backups/sg.tar.gz", trie, journal,
	hatCache.BackupBundleOptions{
		Mode:        hatCache.BackupModeSnapshot,
		KeyPrefixes: []string{"region:sg/"},
	})
```

The prefix list has OR semantics. Prefixes must be non-empty and unique; they
are matched exactly, without trimming or case folding. The selected prefixes
are recorded as `key_prefixes` in `manifest.json`, so operators can see the
scope before restoring it.

Restore is unchanged:

```go
report, err := hatCache.RestoreBackupBundle(
	"/var/backups/sg.tar.gz",
	"/var/lib/hatrie-cache/restore-sg",
	hatCache.BackupBundleRestoreOptions{},
)
```

The restored snapshot contains only the selected keys. A selective snapshot
is a complete snapshot of its selected logical namespace, not a merge or
restore-time filter for an existing full backup. Restore it into an empty
directory unless an intentional replacement is being performed.

Selective prefixes are supported for snapshot bundles, including `auto` mode
when it resolves to a snapshot. They are rejected for Pebble checkpoint and
incremental repository backups because those formats copy the persistent
store as an indivisible recovery unit. They also cannot be combined with the
existing `PartitionLocal` mode; use one explicit scope model per bundle.

The implementation scans the source trie once and applies the prefix
predicate while writing the snapshot. It does not allocate a second trie.
This reduces storage and transfer bytes for narrow namespaces, while CPU and
peak memory still include the source scan and snapshot encoding. See the
selective-backup section in `BENCHMARK.md` for measurements.
