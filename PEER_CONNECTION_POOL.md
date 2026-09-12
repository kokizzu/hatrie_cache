# Peer Connection Pool

`hat/hatPeer` provides an opt-in, protocol-agnostic bounded connection pool.
It is inspired by Tarantool's reusable `net.box` peer connections, but the
pool does not prescribe a wire protocol, authentication scheme, or transport.
The caller supplies a `DialFunc` and wraps its native connection in the small
`hatPeer.Connection` interface.

## Example

```go
pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
	MaxOpen: 8,
	MaxIdle: 8,
	Dial: func(ctx context.Context) (hatPeer.Connection, error) {
		return dialPeer(ctx)
	},
})
if err != nil {
	return err
}
defer pool.Close(context.Background())

err = pool.Do(ctx, func(ctx context.Context, connection hatPeer.Connection) error {
	return connection.(*peerConnection).Request(ctx, request)
})
```

`ConnectionPoolOptions` uses these defaults when the corresponding value is
zero:

| Option | Default | Bound |
| --- | ---: | ---: |
| `MaxOpen` | 8 | 4096 |
| `MaxIdle` | 8, capped at `MaxOpen` | `MaxOpen` |
| `MaxDialAttempts` | 3 | 8 |
| `DialRetryDelay` | 10 ms | none |
| `DialRetryMaxDelay` | 250 ms | must be at least `DialRetryDelay` |

The pool is not installed into any server or client automatically. Configure
it only where peer connection reuse is appropriate. A zero `MaxIdle` selects
the default; this API intentionally does not provide an unbounded or implicit
idle pool.

## Semantics

- `MaxOpen` bounds established connections and therefore bounds concurrent
  handlers that can hold a connection. A caller waits, or its context ends,
  when the bound is reached.
- Successful handlers return connections to the idle pool. A handler error
  closes the connection and is returned unchanged; the pool never retries an
  application operation whose idempotency it cannot determine.
- Dial failures are retried up to `MaxDialAttempts` with exponential delay
  capped by `DialRetryMaxDelay`. Dial retries, like shutdown, require the
  `DialFunc` to honor its context.
- `Close` rejects new work, closes idle connections, and waits for active
  handlers. If its context expires, shutdown continues and a later `Close`
  call can wait for completion.
- `Stats` reports active handlers, established/open connections, idle
  connections, and cumulative acquire/dial counters.
- TLS, authentication, peer identity validation, request deadlines, and
  protocol health checks remain the responsibility of `DialFunc` and the
  wrapped connection.

## Optional Circuit Breaker

Set `ConnectionPoolOptions.Breaker` to enable a per-pool peer circuit breaker.
It is disabled when the pointer is `nil`, which is the default and preserves
the pool's healthy reuse path.

```go
pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
	Breaker: &hatPeer.ConnectionPoolCircuitBreakerOptions{
		FailureThreshold: 5,
		OpenInterval:     5 * time.Second,
	},
	Dial: dialPeer,
})
```

Only non-context dial failures count toward the threshold. Once open, the
breaker rejects dials without invoking `DialFunc`; after the interval it lets
one half-open probe through. A successful probe closes the breaker, while a
failed probe opens it again. `CircuitBreakerStats` exposes state, opens,
rejections, probes, and consecutive failures. Handler errors do not affect
breaker health.

## Benchmark

Measured with `make benchmark-t-peer-pool` on an AMD Ryzen 9 5950X, Linux,
Go's benchmark harness, five samples per case. The benchmark uses an in-memory
fake connection; the handshake case adds deterministic CPU work to model
connection establishment. It is a mechanism benchmark, not a network
throughput claim.

| Case | Time | Memory | Interpretation |
| --- | ---: | ---: | --- |
| Pooled `Do`, no-op dial | 43.98–54.54 ns/op | 0 B/op, 0 allocs/op | Pool coordination floor |
| Dial and close every call, no-op dial | 1.63–1.83 ns/op | 0 B/op, 0 allocs/op | Unrealistically cheap lower bound |
| Pooled `Do`, modeled handshake | 44.27–54.54 ns/op | 0 B/op, 0 allocs/op | One handshake amortized over the run |
| Dial and close every call, modeled handshake | 377.5–406.3 ns/op | 0 B/op, 0 allocs/op | Handshake repeated per operation |

Under the modeled handshake, pooling is approximately **6.9–9.2x faster**.
With a truly free in-process dial, pooling is approximately 25–33x slower due
to its necessary bounds and synchronization. That is a known tradeoff, not a
reason to use a pool for already-local object reuse. In production, retained
memory and file descriptors are bounded by `MaxOpen` and `MaxIdle`; the pool
does not allocate per successful reuse in this benchmark.

For outage protection, the benchmark also compares repeated failed dials with
and without the breaker. With the breaker enabled and a one-failure threshold,
only the first operation reaches `DialFunc`; later operations are rejected
locally until the open interval expires. This bounds reconnect work and is the
primary benefit of the feature, rather than a promise that every rejected
operation is cheaper than an arbitrary synthetic error path.

The measured outage samples were:

| Case | Time | Memory | Dial calls |
| --- | ---: | ---: | ---: |
| Failed dials without breaker | 431.1–447.4 ns/op | 224 B/op, 4 allocs/op | 2.65–2.76 million |
| Failed dials with breaker | 433.7–481.0 ns/op | 224 B/op, 4 allocs/op | 1 |

The enabled breaker has similar local CPU cost, with a measured 0–10% range
relative to the no-breaker failure path, but prevents essentially all repeated
dials after the first failure. It is therefore a resource-protection feature,
not a microbenchmark speedup. The healthy default-off path is unchanged.

## Inspiration

Tarantool documents `net.box` as its client-side connection interface for
interacting with remote instances. This package adopts the reusable, bounded
peer-connection idea while leaving protocol details to the caller:
<https://www.tarantool.io/docs/tarantool/en/3_x/reference/reference_lua/net_box/>.
