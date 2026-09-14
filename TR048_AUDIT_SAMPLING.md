# TR-48 Audit Sampling And Export Sinks

Hatrie Cache audit logging is lossless by default. For high-volume monitoring,
successful events can be sampled while failed or denied events remain fully
recorded. Accepted events can also be sent to structured application-defined
sinks without parsing the JSONL representation.

## Daemon Configuration

The Makefile wrapper exposes the sample rate as `AUDIT_SUCCESS_SAMPLE_RATE`:

```sh
make monitoring-server AUDIT_LOG_PATH=data/audit.jsonl
make monitoring-server AUDIT_LOG_PATH=data/audit.jsonl AUDIT_SUCCESS_SAMPLE_RATE=0.1
```

The equivalent daemon flag is:

```sh
./hatrie-cache -monitoring-server \
  -audit-log-path data/audit.jsonl \
  -audit-success-sample-rate 0.1
```

The default is `0`, which disables sampling and retains every event. Values in
`(0,1)` retain approximately that fraction of successful events. `1` retains
all successful events. Invalid values outside `[0,1]` are rejected. Failed or
denied events (`OK == false`) are always retained regardless of the rate.

The rate is included in `-print-config` output as
`audit_success_sample_rate`. Sampling has no effect when no audit log is
configured.

## Go API

The option is available from both the module root and `hat/hatCache` compatibility
package:

```go
logger, err := hatriecache.NewAuditLoggerWithOptions(writer,
	hatriecache.AuditLoggerOptions{SuccessSampleRate: 0.1})
```

`NewAuditLogger` and `OpenAuditLogger` retain their existing lossless behavior.
`OpenAuditLoggerWithOptions` applies the same options to a mode-0600 JSONL
file.

## Structured Export Sinks

`AuditSink` receives each accepted event synchronously:

```go
logger, err := hatriecache.NewAuditLoggerWithOptions(nil,
	hatriecache.AuditLoggerOptions{
		Sinks: []hatriecache.AuditSink{
			hatriecache.AuditSinkFunc(func(event hatriecache.AuditEvent) error {
				return exportToSecurityPipeline(event)
			}),
		},
})
```

Sinks are called while the logger serializes an event. They should be bounded,
fast, and must not call the same logger recursively. A sink error is returned
from `Log`; other sinks still receive the event, and the local JSONL writer is
also attempted. The existing `Recent` and `Query` APIs retain accepted events
only, so sampled successes are absent from both the local ring and exports.

Sampling is deterministic per logger instance and does not use a global random
source. `SampledSuccessEvents` reports the number of omitted successful events.

## Security And Recovery

- Sampling never removes failures, authorization denials, or other `OK == false`
  events from the audit trail.
- The local JSONL file remains the durable sink when configured; external sinks
  are additive and do not replace it.
- A sink is not a transaction boundary. If an external sink fails after the
  local file succeeds, `Log` reports the error but the local event remains
  available for replay or export.
- Existing file permissions, JSONL encoding, bounded recent retention, and
  audit query limits are unchanged.

## Measurement

On the local AMD Ryzen 9 5950X host, five-sample `-benchmem` results were
measured with `GOMAXPROCS=1`:

| Path | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Default lossless logger, no writer | 139.7 | 32 | 1 | 1.00x |
| 10% successful-event sampling | 22.06 | 3 | 0 | 6.33x faster |
| Lossless logger with no-op structured sink | 160.3 | 32 | 1 | 1.15x slower |

The sampled row is not an apples-to-apples complete audit comparison: it
intentionally omits about 90% of successful events. It demonstrates the
available CPU and allocation reduction for high-volume success traffic. A real
sink's network or queue cost is workload-dependent and is not hidden by this
no-op benchmark. The no-op sink adds about 15% CPU in this run; it adds no
additional measured heap bytes or allocations.

Run the repeatable benchmark with:

```text
make benchmark-tr048-audit-sampling
```
