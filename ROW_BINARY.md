# Row-Binary Fixed-Width Dates

The schema-aware RowBinary format already stores temporal values compactly:

- `SQLRowBinaryDate` uses 4 little-endian bytes containing signed days since
  the Unix epoch. Input timestamps are normalized to their UTC calendar day.
- `SQLRowBinaryDateTime` uses 8 little-endian bytes containing signed Unix
  nanoseconds. Decoded values are returned in UTC.

There are no per-value length prefixes for either type. The fixed-width
contract is verified by `make test-rowbinary-fixed-width` and the broader
`make test` suite.
