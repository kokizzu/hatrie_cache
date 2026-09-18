# T-U37 Replica Applier Throttling

`hatReplication.ApplierThrottle` is an opt-in, bounded token schedule for
replication apply work. It is useful when a replica must catch up without
allowing replay to monopolize foreground CPU and trie write time.

## Usage

```go
throttle, err := hatReplication.NewApplierThrottle(
    hatReplication.ApplierThrottleOptions{
        EntriesPerSecond: 10000,
        Burst:            512,
    },
)
if err != nil {
    return err
}

result, err := hatCache.PullCommandJournal(ctx, trie, journal,
    hatCache.CommandJournalPullOptions{
        Source:                   peerURL,
        ReplicationApplyThrottle: throttle,
    },
)
```

The same throttle can be supplied through `CacheGRPCOptions` for the
replication stream:

```go
server := hatCache.NewCacheGRPCServer(trie, hatCache.CacheGRPCOptions{
    ReplicationApplyThrottle: throttle,
})
```

The throttle waits before each already ordered batch. It never drops,
reorders, or partially applies a batch. A canceled context prevents the batch
from being applied. `nil` is the default and preserves the existing behavior.

`Burst` is the number of entries available immediately after an idle period;
`EntriesPerSecond` is the sustained reservation rate. The throttle is
process-local and should be configured independently on each replica.

## Tradeoff

Throttling deliberately reduces replica catch-up throughput when the limit is
reached. In exchange, it bounds replay admission and lets foreground work run
between batches. It is therefore disabled by default and should be selected
from observed replica lag and foreground tail-latency requirements.

Reservations use a fixed-size mutex-protected schedule and report `0 B/op`
and `0 allocs/op` in the benchmark below. A cancellation after a reservation
has been made is conservative: the reserved capacity remains consumed, which
prevents an immediately retried caller from exceeding the configured rate.

## Measurement

Commands:

```text
make baseline-tu37-applier-throttle
make benchmark-tu37-applier-throttle
```

Five `-benchmem` samples were collected on Linux/amd64 with an AMD Ryzen 9
5950X. The unthrottled admission loop is a matched control; the reservation
path is the opt-in cost.

| Path | Raw ns/op samples | Median ns/op | Memory/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Before, unthrottled | `0.2781 0.2814 0.2504 0.2532 0.2398` | `0.2532` | `0 B` | `0` |
| After, unthrottled | `0.2393 0.2520 0.2455 0.2302 0.2278` | `0.2393` | `0 B` | `0` |
| After, opt-in reservation | `31.30 31.64 35.88 31.62 32.45` | `31.64` | `0 B` | `0` |

The matched legacy control is `1.06x` faster after the change, within normal
benchmark noise. The opt-in reservation costs about `31.64 ns` per batch
reservation; that is the explicit CPU tradeoff for bounded replay admission,
while the default nil path remains allocation-free and unchanged in behavior.

Focused validation:

```text
make test-tu37-applier-throttle
make test-tu37-applier-throttle-cache
make race-tu37-applier-throttle
make race-tu37-applier-throttle-cache
make vet-tu37-applier-throttle
make vet-tu37-applier-throttle-cache
make test-tu37-applier-throttle-package
make test-tu37-applier-throttle-cache-package
```
