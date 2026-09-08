# HTTP Protocol Compatibility

The monitoring command endpoint uses the version headers exposed by
`hatCommand`:

- `X-Hatrie-Protocol-Version` accepts an exact version or inclusive range.
- `X-Hatrie-Protocol-Supported` reports the server's configured range.

`MonitoringOptions.ProtocolVersions` configures the inclusive range accepted by
the monitoring HTTP server. Its zero value defaults to the current supported
range (`1`), so existing clients that omit the request header continue to work.
An invalid header returns `400 Bad Request`; a valid but incompatible range
returns `426 Upgrade Required`. Successful responses include the selected
version, the supported range, and `Vary: X-Hatrie-Protocol-Version` in addition
to the normal content-encoding variation.

Replication clients can set `HTTPReplicatorOptions.ProtocolVersions` to
advertise their compatible range on HTTP replication writes. Its zero value
omits the header for legacy peer compatibility. Compliant peers can reject a
replication request whose range does not overlap their configured server range.
The setting affects HTTP replication requests only; gRPC uses its separate
`CacheGRPCOptions.ProtocolVersions` metadata gate.

Protocol negotiation selects the highest version in the intersection. Version
selection is a compatibility gate, not a schema migration or a distributed
consensus protocol. Operators must keep the selected wire contract compatible
until all clients and replicas have moved to the next version.
