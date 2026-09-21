# TT-037 Audit Metadata Redaction

`hatAudit` already provides bounded recent events, JSONL append-only output,
and optional sinks. This feature adds an opt-in security mode that removes
common command payloads before any of those destinations receive the event.

## Usage

Use the option when constructing an existing logger:

```go
logger, err := hatAudit.OpenAuditLoggerWithOptions(path, hatAudit.AuditLoggerOptions{
	RedactSensitive: true,
})
```

For an in-memory logger, use `NewRedactedAuditLogger`. The convenience file
constructor is `OpenRedactedAuditLogger`.

`RedactAuditEvent` is also available as a pure helper for sinks that need to
apply the same policy before forwarding events.

## Redaction Boundary

Redacted events preserve action, command name, node, protocol, method, status,
success state, and remote address. Non-empty `Key` and `Message` fields become
`[REDACTED]`; URL query and fragment data are removed from `Path`; `Details` is
dropped completely because its caller-defined values cannot be classified
safely by the logger.

Redaction happens before recent retention, JSONL writes, and synchronous sink
callbacks. The default constructors remain lossless for compatibility; use
the redacted constructors or `RedactSensitive: true` for security-sensitive
streams.

## Measured Cost

Five 500 ms samples on Linux/amd64, AMD Ryzen 9 5950X, with no file writer:

| Path | Median time | Memory | Allocations | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing logger | 160.6 ns/op | 32 B/op | 1 | 1.00x |
| Redacted logger | 171.9 ns/op | 32 B/op | 1 | 1.07x |

The security path adds about 7% CPU in this small event shape without an
allocation or memory increase. It is opt-in so callers can choose the existing
lossless behavior when metadata is already controlled.

Verification:

```text
make test-tt037-audit-redaction
make race-tt037-audit-redaction
make verify-tt037-audit-redaction
make benchmark-tt037-audit-redaction
```
