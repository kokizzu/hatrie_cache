# T241: Role-Based and Per-Space Authorization

The compact peer session already requires connection-level authentication via
`CompactPeerListenerOptions.Authorize`. T241 adds an opt-in request-level
authorization boundary so a successfully authenticated connection cannot call
every command or access every logical space.

## Model

- The connection owner supplies trusted `PeerID` and `Roles` in
  `CompactPeerSessionOptions`.
- `AuthorizeRequest` runs after the connection handshake and before the
  application handler.
- `SpaceExtractor` maps the opaque command payload to a logical space when a
  policy needs space-level decisions.
- `CompactPeerRoleAuthorizer` is default-deny. A request is allowed only when
  one configured role, command, and space rule matches.
- `Command: "*"` matches every command. Empty `Space` or `Space: "*"` matches
  every space. Roles are exact matches; there is no implicit role inheritance.

Example:

```go
policy, err := hatPeer.NewCompactPeerRoleAuthorizer(
	hatPeer.CompactPeerRoleAuthorizerOptions{
		Rules: []hatPeer.CompactPeerRoleRule{
			{Role: "reader", Command: "get", Space: "tenant-a"},
			{Role: "writer", Command: "put", Space: "*"},
		},
	},
)
if err != nil {
	return err
}

sessionOptions := hatPeer.CompactPeerSessionOptions{
	PeerID: "peer-a",
	Roles:  []string{"reader"},
	SpaceExtractor: func(_ context.Context, request hatPeer.CompactFrame) (string, error) {
		return string(request.Payload), nil
	},
	AuthorizeRequest: policy.Authorize,
	Handler: handler,
}
```

`Roles` and `PeerID` are server-side configuration. They must be selected from
the already-authenticated connection identity, not from request payload data.
For a listener serving one principal, configure those values in its static
`Session` options. A deployment serving many principals can construct sessions
directly after its own connection-to-principal mapping.

Denied requests never reach `Handler` and return the stable
`ErrCompactPeerRequestDenied` message. Parser or policy details are not sent to
the peer. Caller cancellation and deadlines still propagate. The authorization
callback receives borrowed command and payload slices and must not retain or
mutate them.

The default remains unchanged: a nil `AuthorizeRequest` skips request policy
and does not add role or space allocations to ordinary sessions. The wire
format is unchanged, so old compact peers remain interoperable.

## Benchmark

This microbenchmark compares a no-op callback shape with one exact role,
command, and space rule. Five samples were run on AMD Ryzen 9 5950X with
`-benchmem`:

| Path | Median ns/op | B/op | Allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| No-op authorization baseline | 0.2698 | 0 | 0 | 1.00x |
| One-rule role authorizer | 12.45 | 0 | 0 | 46.1x the no-op baseline |

The enabled policy path is allocation-free, but it necessarily costs CPU for
role/command/space matching. This is an authorization correctness and isolation
feature, not a throughput optimization; leave it disabled only when the
connection is already isolated to a trusted command/space boundary.
