# TTG42 Per-Peer Adaptive Flow Control

`hatReplication.NewPeerRelayBackpressure` provides bounded, independent lag
hysteresis for replication peers. It is useful when one slow downstream node
must be paused without stopping admission for healthy peers.

```go
controller, err := hatReplication.NewPeerRelayBackpressure(
	hatReplication.PeerRelayBackpressureOptions{
		Enabled: true,
	},
)
if err != nil {
	return err
}

decision, err := controller.Observe("region-eu", downstreamLag)
if err != nil {
	return err
}
if !decision.Allowed {
	// Retain or defer work for this peer until it recovers.
}
```

## Defaults And Bounds

The controller is disabled by default. Enabling it uses the existing relay
watermarks: pause at 10,000 journal entries and resume at 5,000. State is
bounded to 1,024 peers by default, with 256 bytes per peer name. Both limits
are configurable within hard upper bounds.

Peer names are trimmed and validated. A full peer does not affect another
peer's state: each peer has its own pause bit, lag, and transition count.
`Snapshot` returns a sorted copy for deterministic monitoring, and `Remove`
releases a peer's retained state.

The controller owns no queue and never drops data. Callers decide how to retain
or defer work after an `Observe` decision. Existing single-relay
`RelayBackpressure` behavior is unchanged.

## Measured Tradeoff

`make m237-tt-g42-benchmark` ran five samples on the repository's AMD Ryzen 9
5950X host. The per-peer benchmark round-robins 16 already-registered peers:

| Operation | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing single-relay admission | 6.905 | 0 | 0 | 1.00x |
| Per-peer registry observation | 32.42 | 0 | 0 | 4.70x |

The registry's map lookup and shared mutex cost roughly 25.5 ns per observation,
but it retains no per-observation heap data and imposes no cost while disabled.
The tradeoff is appropriate for an opt-in admission control path, not for a
per-row data-plane loop.

Focused tests cover hysteresis isolation, bounds, removal, disabled behavior,
and deterministic snapshots in
`hat/hatReplication/tt42_peer_flow_control_test.go`.
