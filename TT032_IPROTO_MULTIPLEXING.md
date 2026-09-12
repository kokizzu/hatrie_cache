# TT-032 IProto-Style Command Multiplexing

`CacheService.CommandStream` now supports optional request correlation for
clients that need more than one command in flight.

## Protocol

Set `CommandRequest.request_id` to a nonzero value. The server copies that
value to `CommandResponse.request_id`, so responses can be matched without
assuming response order. Request IDs only need to be unique while their
responses are in flight.

The first request selects the stream mode:

- A zero `request_id` keeps the legacy serialized stream. Every response is
  sent in request order and later nonzero IDs are rejected.
- A nonzero `request_id` enables correlated mode. Every request on that stream
  must use a nonzero ID, and responses may arrive in any order.

Correlated mode is intended for independent operations. A client must not use
it for a sequence whose correctness depends on write order, such as `SET`
followed by `GET` of the same key, unless it provides its own ordering.

## Backpressure And Defaults

`CacheGRPCOptions.CommandStreamWorkers` bounds the number of commands in
flight per stream. The default is `1`, which preserves the existing execution
and memory profile. Values above `64` are clamped. The server only starts the
correlated worker path after the client sends a nonzero request ID.

The worker bound also limits queued decoded requests. A slow receiver applies
backpressure through the bounded result path instead of allowing an unbounded
response queue. Authentication, authorization, audit, write protection, and
command journaling continue to run for each command.

## Tradeoff

The reproducible benchmark is:

```sh
make benchmark-tt032-multiplexing
```

It sends 32 independent missing-key reads before receiving their responses.
Five one-second samples on Linux amd64 produced these medians:

| Mode | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Legacy ordered, 32 requests | 106,548 | 40,317 | 857 |
| Multiplexed, 4 workers, 32 requests | 127,010 | 48,953 | 914 |

For this tiny in-process workload, multiplexing is `1.19x` slower, uses
`1.46x` more bytes, and performs `1.07x` as many allocations. The cost comes
from bounded coordination and response correlation; it is the price of
allowing independent commands to overlap when individual commands are
latency-bound. The default serialized path is unchanged and remains the
recommended mode for small local operations.

## Compatibility

Adding fields `13` and `6` to `CommandRequest` and `CommandResponse` is
backward-compatible with protobuf clients that do not know about them. Such
clients continue to send zero IDs and use the legacy ordered path.
