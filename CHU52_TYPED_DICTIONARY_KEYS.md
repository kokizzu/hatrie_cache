# CH-U52 Typed Dictionary Keys

`hatSql.SQLExternalDictionary` supports two snapshot loaders:

- `Load` keeps the original `map[string]interface{}` contract.
- `LoadEntries` publishes typed keys without formatting them into strings.

The typed loader is selected with `KeyKind`:

```go
dictionary, err := hatSql.NewSQLExternalDictionary(hatSql.SQLExternalDictionaryOptions{
	Name:    "region_limits",
	KeyKind: hatSql.SQLExternalDictionaryKeyInt64,
	LoadEntries: func(context.Context) ([]hatSql.SQLExternalDictionaryEntry, error) {
		return []hatSql.SQLExternalDictionaryEntry{
			{Key: int64(7), Value: int64(100)},
			{Key: int64(8), Value: int64(200)},
		}, nil
	},
})
if err != nil {
	return err
}
if err := dictionary.Refresh(context.Background()); err != nil {
	return err
}
value, found, err := dictionary.LookupKey(int32(7))
```

Supported kinds are `SQLExternalDictionaryKeyString`,
`SQLExternalDictionaryKeyInt64`, `SQLExternalDictionaryKeyUint64`, and
`SQLExternalDictionaryKeyTime`. Signed and unsigned integer widths are
normalized to the configured kind when the conversion is lossless. Time keys
use `time.Time.UnixNano()`, so equivalent instants match regardless of their
location representation.

`DICT_GET`, `DICT_GET_OR_DEFAULT`, and `DICT_HAS` use the same typed lookup
path. The dictionary name remains a string; the key expression must match the
configured kind. Invalid keys return `ErrSQLExternalDictionaryKeyType`, and
duplicate normalized keys are rejected before a snapshot is published.

The old `Load` API remains the fallback for string-keyed dictionaries. The two
loader fields are mutually exclusive, failed refreshes retain the last good
snapshot, and values are cloned before publication. Byte slices are copied on
lookup; scalar values are returned without a per-lookup allocation.

## Configuration Rules

`KeyKind` defaults to `SQLExternalDictionaryKeyString`. `Load` may only be
used with that kind. Use `LoadEntries` for numeric or time keys. A typed
snapshot is replaced atomically on refresh, and `Stats().Entries` reports the
number of normalized typed keys.

## Measurements

The focused benchmark runs five `-benchmem` samples on an AMD Ryzen 9 5950X
with 1,024 `int64` entries and a scalar lookup value. The final typed path has
a 35.44 ns/op median, 0 allocs/op, and 6 B/op. The legacy string-loader
benchmark is 13.80 ns/op median with 0 B/op and 0 allocs/op. The raw 8.03 ns/op baseline is
a direct `map[int64]interface{}` lookup and does not include dictionary
snapshot publication, readiness/lifecycle checks, value isolation, or the
public interface-key conversion. The first composite-key implementation was
98.22 ns/op; the final scalar-map and hot-path changes are 2.77x faster than
that implementation while preserving the default legacy path.
