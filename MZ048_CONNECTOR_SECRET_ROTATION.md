# MZ-048 Connector Secret Rotation

MZ-048 adds an opt-in credential handoff for maintained source and sink
connectors. A connector can publish a new credential while it remains in its
current lifecycle state; the registry does not call `Stop`, `Pause`, `Start`,
or `Resume` as part of the handoff.

## API

Implement `hatPipeline.ConnectorCredentialRotator` alongside the existing
`hatPipeline.Connector` interface:

```go
type connector struct {
	// The connector's data plane owns its atomic credential publication.
}

func (connector *connector) RotateCredentials(
	ctx context.Context,
	rotation hatPipeline.ConnectorCredentialRotation,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Validate and publish rotation.Value atomically for new requests.
	return nil
}
```

Register and start the connector normally, then call:

```go
err := registry.RotateCredentials(ctx, "orders-source", hatPipeline.ConnectorCredentialRotation{
	Version: 2,
	Value:   nextCredential,
})
```

The registry serializes the call with lifecycle transitions for that connector
and rejects version zero. The default maximum credential size is 1 MiB and can
be lowered or raised up to 64 MiB through
`ConnectorRegistryOptions.MaxCredentialBytes`. Empty values are allowed for
connectors that use an empty credential to disable authentication.

The registry validates but does not copy or retain `Value`; ownership passes to
the optional rotator after the method returns. Callers should pass a private
copy, such as the value returned by `hatAuth.ResolvedConnection.SecretValue`,
and must not mutate it after the call. The rotator must atomically publish its
own copy if it needs to retain the credential.

Connectors that do not implement the optional interface return
`ErrConnectorCredentialRotationUnsupported` and keep the existing lifecycle
behavior. Failed and stopped connectors reject rotation; created, running, and
paused connectors may accept it so a credential can be installed before the
first start.

## Secret registry integration

`hatAuth.ResourceRegistry.RotateSecret` remains the compare-and-swap source of
truth for named secrets. Resolve the new version and hand its value to the
connector registry explicitly. This keeps resource ownership and connector
lifecycle ownership separate, and avoids logging or serializing secret bytes.
There is no implicit cross-registry coupling, distributed secret manager, or
automatic retry after a connector rejects a credential.

## Benchmark

The benchmark compares the existing pause/resume interruption fallback with
the new in-place registry path using the same no-op connector and a 14-byte
credential. Five runs were measured on an AMD Ryzen 9 5950X, `-cpu=1`:

| Path | Raw ns/op | Median ns/op | B/op | allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing pause + resume | 149.0, 150.0, 153.2, 149.8, 135.2 | 150.0 | 0 | 0 | 5.12x slower |
| In-place rotation | 30.22, 29.04, 29.31, 28.91, 29.97 | 29.31 | 0 | 0 | 1.00x |

The in-place path is therefore about 5.1x faster for this control-plane
operation and avoids the availability interruption. The benchmark does not
measure a connector's network handshake or its own credential parsing.

Run it with:

```text
make benchmark-mz048-secret-rotation-baseline
make benchmark-mz048-secret-rotation-inplace
```
