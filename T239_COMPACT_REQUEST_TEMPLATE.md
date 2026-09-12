# Compact Request Templates

`hatPeer.NewCompactRequestTemplate` prepares immutable command metadata for a
repeated compact-protocol request. `MarshalInto` appends a request to a caller-
owned buffer, so a caller can reuse the same buffer for high-rate replication
or peer traffic without allocating a new frame each time.

```go
protocol, err := hatPeer.NewCompactProtocol(hatPeer.CompactProtocolOptions{})
if err != nil {
    return err
}
template, err := hatPeer.NewCompactRequestTemplate([]byte("SETSTR"))
if err != nil {
    return err
}

buffer := make([]byte, 0, 128)
for requestID, payload := range requests {
    buffer, err = template.MarshalInto(protocol, requestID, payload, buffer[:0])
    if err != nil {
        return err
    }
    if _, err := connection.Write(buffer); err != nil {
        return err
    }
}
```

`CompactMultiplexer.RequestTemplate` provides the same prepared command to the
correlated request path. The template copies the command at construction time,
so later mutation of the caller's input cannot alter emitted frames. The wire
format and request validation rules are unchanged.

## Measurement

The existing compact frame benchmark measured `39.6-41.1 ns/op`, `80 B/op`,
and one allocation. In a paired request fixture, direct marshal measured
`37-38 ns/op`, `31-32 B/op`, and one allocation; prepared marshal into a reused
buffer measured `23-25 ns/op`, `0 B/op`, and zero allocations. That is about
`1.5x` lower latency and complete per-request allocation elimination. The
optimization is opt-in; callers that use `CompactProtocol.Marshal` retain the
existing behavior.
