# String Dictionary Codec

`EncodeSQLStringDictionary` and `DecodeSQLStringDictionary` provide an
explicit dictionary encoding for one string column. The `HSDC` format stores
first-seen unique strings once, followed by unsigned IDs. Decoded repeated
values reuse dictionary strings instead of copying each value.

```go
wire, err := hatSql.EncodeSQLStringDictionary(regions)
if err != nil {
    return err
}
regions, err = hatSql.DecodeSQLStringDictionary(wire)
```

The codec is deterministic, caps values at one million and dictionary bytes
at 64 MiB, rejects duplicate dictionary entries and invalid IDs, and does not
change existing RowBinary or storage formats. It has no NULL marker; callers
with nullable columns should encode NULL separately.

## Measurement

The benchmark uses 1,024 strings and compares the dictionary format with the
existing length-prefixed representation. Run it with:

```text
make benchmark-sql-string-dictionary
```

Representative five-sample ranges on the repository benchmark host:

| Workload | Path | Time | Wire bytes | Allocated bytes | Allocations |
| --- | --- | ---: | ---: | ---: | ---: |
| Four repeated values | Raw encode | 10.8-11.2 us | 8,448 | 34,296 | 15 |
| Four repeated values | Dictionary encode | 13.7-13.9 us | 1,064 | 10,560 | 3 |
| Four repeated values | Raw decode | 24.7-25.5 us | 8,448 | 67,952 | 1,036 |
| Four repeated values | Dictionary decode | 6.0-6.3 us | 1,064 | 18,528 | 6 |
| 1,024 unique values | Raw encode | 12.8-13.3 us | 10,112 | 46,584 | 16 |
| 1,024 unique values | Dictionary encode | 116.7-119.2 us | 12,040 | 203,308-203,311 | 33 |
| 1,024 unique values | Raw decode | 28.1-29.0 us | 10,112 | 75,120 | 1,036 |
| 1,024 unique values | Dictionary decode | 65.9-78.4 us | 12,040 | 106,832 | 1,031 |

Use this codec for low-cardinality or repeated columns. For high-cardinality
columns, the raw format is faster and smaller; no global default is changed.
