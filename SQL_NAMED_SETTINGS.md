# SQL Named Settings Collections

`hatSql` provides versioned named settings profiles for query and storage
callers. A profile is a bounded map of string values, optionally inheriting
from another profile, and published as an immutable atomic snapshot. This
follows ClickHouse named collections while keeping parsing and application of
domain-specific settings with the caller.

## Usage

```go
registry, err := hatSql.NewSQLNamedSettingsRegistry(hatSql.SQLNamedSettingsRegistryOptions{})
if err != nil {
    return err
}

_, err := registry.PutProfile("base", hatSql.SQLNamedSettingsProfile{
	Values: map[string]string{
		"max_rows": "100000",
		"timeout":  "5s",
	},
})
if err != nil {
	return err
}

profile, err := registry.PutProfile("analytics", hatSql.SQLNamedSettingsProfile{
	Parent: "base",
	Values: map[string]string{
		"max_rows": "200000",
		"format":   "columnar",
	},
})
if err != nil {
	return err
}

resolved, err := registry.Resolve("analytics", map[string]string{
	"timeout": "10s",
})
if err != nil {
    return err
}
applyQueryOrStorageSettings(resolved.Values)

if timeout, ok := registry.LookupValue("analytics", "timeout"); ok {
    applyTimeout(timeout)
}

_, err = registry.PutProfileIfRevision("analytics", profile.Revision, hatSql.SQLNamedSettingsProfile{
	Parent: "base",
	Values: map[string]string{
		"max_rows": "300000",
		"format":   "columnar",
	},
})

```

`Put` and `PutIfRevision` remain parentless compatibility APIs. `PutProfile`
and `PutProfileIfRevision` add an optional parent; inherited values are applied
from the oldest parent to the child, then caller overrides are applied by
`Resolve`. The returned revision is the highest publication revision in the
effective parent chain, so a parent update is visible to readers without
rewriting every child.

Set `ValidateSetting` when the registry owns a known SQL or storage setting
schema. The validator runs before publication and for `Resolve` overrides;
returning an error rejects the operation with
`ErrSQLNamedSettingsSettingInvalid`.

```go
registry, err := hatSql.NewSQLNamedSettingsRegistry(hatSql.SQLNamedSettingsRegistryOptions{
	ValidateSetting: func(key, value string) error {
		if key != "max_rows" && key != "timeout" {
			return hatSql.ErrSQLNamedSettingsSettingInvalid
		}
		return nil // Parse the value according to the application's schema.
	},
})
```

Parents must exist when a profile is published. Cycles and excessive depth
are rejected, and a parent cannot be deleted while a direct child refers to
it. `DeleteIfRevision` and the profile publication methods provide
compare-and-swap updates; a stale writer receives
`ErrSQLNamedSettingsConflict` instead of silently overwriting a newer profile.

## Bounds And Defaults

- The registry has no goroutine and adds no default SQL execution work.
- The zero value is usable.
- Defaults are 256 collections, 128 settings per collection, and 16 KiB per
  value, with an eight-link inheritance depth.
- Configured limits are validated and bounded to prevent accidental excessive
  memory reservation.
- The inheritance depth can be configured up to 64 links with
  `MaxInheritanceDepth`.
- Collection names and setting keys are bounded to 256 bytes.
- `Lookup` and `Snapshot` return copies, so callers cannot mutate published
  state. `LookupValue` reads one immutable string without cloning the profile.
- `Stats` reports revision and counts without exposing configuration values.

## Benchmark

The benchmark compares inherited resolution with a manual three-map merge.
The inherited path has the same measured `B/op` and allocation count as that
manual merge after using a bounded stack-backed parent chain. Parentless
resolution retains the existing allocation profile; inherited resolution adds
only parent traversal CPU.

See [BENCHMARK.md](BENCHMARK.md#ch-001-named-settings-profile-inheritance-and-validation)
for every raw sample and the median table.
