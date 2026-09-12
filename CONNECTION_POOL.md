# Connection Pool

`hatReplication.ConnectionPool[T]` is an importable, protocol-neutral pool for
long-lived peer clients. It adopts the Net.box-style reuse and bounded in-flight
connection model without choosing an authentication, TLS, HTTP, or gRPC
implementation for callers.

## Configuration

```go
pool, err := hatReplication.NewConnectionPool(
    hatReplication.ConnectionPoolOptions[*Client]{
        MaxOpen: 4,
        MaxIdle: 4,
        Dial: func(ctx context.Context) (*Client, error) {
            return dialClient(ctx)
        },
        Close: func(client *Client) error {
            return client.Close()
        },
    },
)
```

`MaxOpen` is required and bounds created connections. `MaxIdle` bounds
reusable released connections; zero disables idle retention and closes every
released connection. A value larger than `MaxOpen` is reduced to
`MaxOpen`. The pool does not dial during construction.

## Lifecycle

```go
client, err := pool.Acquire(ctx)
if err != nil {
    return err
}
result, err := client.Do(ctx, request)
if err != nil {
    pool.Discard(client)
    return err
}
pool.Release(client)
return result
```

`Acquire` first reuses an idle connection, otherwise waits for capacity. It
returns the caller context error on cancellation and
`ErrConnectionPoolClosed` after `Close`. `Release` returns a healthy client to
the idle pool. `Discard` closes a failed client and immediately returns its
capacity so another acquire can dial a replacement.

`Close` is idempotent, wakes blocked acquirers, and closes idle clients. It does
not wait for active clients; owners must still call `Release` or `Discard`, and
those clients are closed after the pool is closed. The caller's `Dial` function
owns reconnect, backoff, endpoint selection, authentication, and TLS policy.

`Stats` reports configured limits and point-in-time open, idle, in-use, and
closed state. The pool is safe for concurrent use.

## Benchmark

Run:

```text
make benchmark-connection-pool
```

The synthetic benchmark uses side-effecting in-process dial/close stubs. Pool
reuse is about 43 ns/op with zero steady-state allocations; the direct stub
control is about 3.6 ns/op. Real connection setup, TLS, and authentication are
outside this microbenchmark and are the reason to use the pool. See the raw
samples and limitation in [BENCHMARK.md](BENCHMARK.md#connection-pool-reuse).
