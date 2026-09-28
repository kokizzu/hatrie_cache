# Priority Queue Visibility Leases

T245 adds opt-in visibility timeouts for priority queues. A worker can claim an
item, process it, and acknowledge it. If the worker stops before the
acknowledgement, the item becomes visible again after the timeout.

The feature is disabled by default. Existing `POPPQ` behavior remains
destructive, and ordinary priority queues do not allocate lease state until the
first claim.

## Go API

```go
lease, ok, err := cache.ClaimPriorityQueueChecked("jobs", 30*time.Second)
if err != nil || !ok {
	return err
}

// Process lease.Item, then acknowledge the exact opaque token.
acked, err := cache.AckPriorityQueueChecked("jobs", lease.Token)
```

`ClaimPriorityQueue` returns the next item according to the existing priority
ordering. The claimed item is hidden from `PEEKPQ`, `GETPQ`, and other claims
until it is acknowledged or its visibility interval expires. A positive
visibility duration is required.

## Commands

Claim with a positive `ttl_seconds`:

```json
{"command":"CLAIMPQ","key":"jobs","ttl_seconds":30}
```

The response value is JSON containing the opaque token, item, and expiration:

```json
{
  "ok": true,
  "message": "claimed priority queue item",
  "value": "{\"token\":\"1\",\"item\":{\"priority\":10,\"value\":\"job-17\"},\"expires_at\":\"2026-09-29T12:00:30Z\"}"
}
```

Acknowledge with the token returned by `CLAIMPQ`:

```json
{"command":"ACKPQ","key":"jobs","value":"1"}
```

Successful acknowledgement returns `value: "1"`. An unknown or expired token
returns `value: "0"` without deleting a newly requeued item. `CLAIMPRIORITY`
and `ACKPRIORITY` are accepted aliases.

## Recovery Semantics

- Expired leases are reinserted with their original priority and ordering
  sequence before the next queue read or claim.
- Acknowledgement is idempotent from the caller's perspective: a repeated or
  stale token does not acknowledge another item.
- Snapshots and replication include claimed items as available work, but do not
  persist lease ownership or tokens. After restart, a claimed item can be
  delivered again, which is at-least-once behavior.
- Lease tokens are in-memory opaque identifiers, not authentication
  credentials. Transport authentication and authorization still apply.

## Cost

The default queue header grows by one map word, from 32 to 40 bytes on the
benchmark platform. The lease map is allocated lazily on the first claim and
retained for a queue that has used visibility leases, avoiding an allocation on
each claim/ack cycle. The priority item remains 48 bytes.

On the benchmark platform, a ten-sample paired run measured:

| Workload | Clean baseline | T245 candidate | Change |
| --- | ---: | ---: | ---: |
| Existing push/pop | 552.2 ns/op, 64 B/op, 2 allocs/op | 519.7 ns/op, 64 B/op, 2 allocs/op | 1.06x throughput, no allocation change |
| Claim + acknowledge | Not applicable | 912.1 ns/op, 20 B/op, 2 allocs/op | Opt-in path |

The existing push/pop medians are within normal benchmark variance; T245 does
not increase its allocations. The claim/ack path is intentionally more work
than destructive pop because it tracks ownership and expiry.

Raw samples and the exact benchmark command are recorded in
[BENCHMARK.md](BENCHMARK.md#t245-priority-queue-visibility-leases).
