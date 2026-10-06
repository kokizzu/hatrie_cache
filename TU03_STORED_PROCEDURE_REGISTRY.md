# T-U03 Stored Procedure Registry

`hatSql.StoredProcedureRegistry` is an importable, opt-in registry for trusted
in-process callbacks. It gives a callback a stable package/name/version
identity and puts bounded authorization, admission, payload copying, and panic
conversion around each call.

It does not add SQL syntax, transport exposure, persistence, or a sandbox. The
caller decides whether and how a registry is made available to SQL or a
network service. Use a sandboxed runtime for untrusted code; this registry is
not one.

## Example

```go
registry, err := hatSql.NewStoredProcedureRegistry(hatSql.StoredProcedureRegistryOptions{
	MaxProcedures:      1024,
	MaxArguments:       16,
	MaxInputBytes:      64 << 10,
	MaxOutputBytes:     64 << 10,
	MaxConcurrentCalls: 32,
	MaxCallDuration:    2 * time.Second,
	Authorize: func(ctx context.Context, request hatSql.StoredProcedureAuthorization) error {
		if request.Principal == "" {
			return hatSql.ErrStoredProcedureUnauthorized
		}
		return nil
	},
})
if err != nil {
	return err
}

err = registry.Register(hatSql.StoredProcedureDefinition{
	Package: "math",
	Name:    "sum",
	Version: "v1",
	Evaluate: func(ctx context.Context, arguments []interface{}) (interface{}, error) {
		return arguments[0].(int64) + arguments[1].(int64), nil
	},
})
if err != nil {
	return err
}

value, err := registry.Call(ctx, "report-reader", "MATH", "SUM", "v1", []interface{}{int64(2), int64(3)})
// value is int64(5); package and name lookup is case-insensitive.
```

Registration is immutable. Register a new version for a compatible rollout;
registering the same normalized package/name/version returns
`ErrStoredProcedureDuplicate`.

## Bounds And Safety

The constructor returns an error for negative limits. Zero uses these defaults:

| Limit | Default |
| --- | ---: |
| Registered procedures | 1,024 |
| Positional arguments | 64 |
| Input payload | 1 MiB |
| Output payload | 1 MiB |
| Concurrent calls | 64 |
| Call duration | 5 seconds |

Package and procedure names are trimmed and normalized to lower case. Versions
are trimmed but remain case-sensitive. Each identifier is valid UTF-8, rejects
control characters, and is limited to 128 bytes.

An authorizer receives the normalized identity and caller principal before
argument admission. A nil authorizer permits trusted embedded use; callers
exposing procedures across a trust boundary should always provide one.

The registry copies byte slices and supported compound values before invoking a
callback and copies the returned value before giving it to the caller. This
prevents a callback from mutating the caller's input buffer or retaining the
registry's returned buffer. Supported values are nil, strings, booleans,
signed/unsigned integers, floats, byte slices, and bounded slices of those
values. Unsupported or cyclic values return an invalid-value error.

Callback panics become `ErrStoredProcedurePanic`. The callback receives a
derived context with the configured deadline, but deadline enforcement is
cooperative: arbitrary Go code cannot be forcibly interrupted. Admission waits
for the configured concurrency slot or returns the context error.

## Benchmark

Five `-benchmem -count=5` samples were measured on an AMD Ryzen 9 5950X. The
direct callback is only a lower-bound control; it does not perform lookup,
authorization, copying, bounds checks, timeout setup, or concurrency admission.

| Operation | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Direct callback control | 2.198 | 0 | 0 | 1.00x |
| Bounded registry call | 884.0 | 432 | 8 | 402.2x slower |

Raw samples:

```text
direct_callback_ns: 2.205 2.197 2.198 2.223 2.192
registry_call_ns: 886.2 890.5 884.0 876.3 864.6
registry_call_heap: 432 B/op, 8 allocs/op
```

The overhead is isolated to callers that opt into this safety boundary; the
existing `VersionedFunction` and SQL execution paths are unchanged. For a hot
trusted callback that already has its own admission and authorization, call it
directly instead of paying for this wrapper.
