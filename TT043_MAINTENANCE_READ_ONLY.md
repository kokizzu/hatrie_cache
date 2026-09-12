# TT-043 Maintenance Read-Only Mode

`MaintenanceReadOnly` is a default-off safety mode for a node that must keep
serving reads and backups while public cache writes are paused.

## Enable It

Library users set the option on each public server they expose:

```go
grpcServer := hatCache.NewCacheGRPCServer(trie, hatCache.CacheGRPCOptions{
	MaintenanceReadOnly: true,
})

monitoring := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
	MaintenanceReadOnly: true,
}).Handler()
```

The `hatrie-cache` binary accepts the equivalent flag:

```text
hatrie-cache --maintenance-read-only
```

It can also be set in the JSON config file:

```json
{
  "maintenance_read_only": true
}
```

The default is `false`. The effective CLI value is shown as
`maintenance_read_only` by `--print-config` and `/api/config`.

## Behavior

The mode rejects public, journaled cache commands through:

- gRPC `Command`, `CommandStream`, and `CommandBatchStream`;
- HTTP `/api/commands`, including asynchronous submissions;
- mixed batches before execution, so a batch cannot partially mutate the
  cache.

Reads, health, entries, SQL reads, metrics, snapshots, and backup,
backup-verification, and backup-rehearsal endpoints remain available. HTTP
write rejections return `423 Locked`; gRPC write rejections return
`FailedPrecondition`. Both use the message `node is in maintenance read-only
mode`.

This option is deliberately narrower than `WriteProtected`. The existing
`WriteProtected` behavior and precedence are unchanged. Internal replication
commands, direct in-process trie calls, journal replay, and restore APIs are
not controlled by this public traffic gate; they retain their existing
authentication and safety checks so a maintenance node can converge and be
recovered.

The monitoring endpoint exports
`hatrie_cache_maintenance_read_only_enabled{node="..."}` for operational
discovery.

## Disable or Roll Back

Restart with `--maintenance-read-only=false`, remove the JSON option, or set
the library option back to `false`. There is no data migration and no cache
format change.
