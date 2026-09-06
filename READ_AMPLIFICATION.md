# Read Amplification Metrics

`hat/hatMetrics.ReadAmplificationRegistry` tracks bytes read and bytes
returned for each `(part, column)` pair. This makes it possible to identify
parts or columns where predicates cause substantially more data to be read
than is returned.

```go
registry := hatMetrics.NewReadAmplificationRegistry()
if err := registry.Record("part-001", "payload", 4096, 512); err != nil {
	return err
}

for _, row := range registry.Snapshot() {
	fmt.Printf("%s.%s: %.2fx\n", row.Part, row.Column, row.Ratio())
}
```

`Record` accepts cumulative byte observations and increments
`ReadOperations`. Names are trimmed and both names are required. `Snapshot`
returns independent rows sorted by part and column, which makes exports and
tests deterministic. `Ratio` is `BytesRead / BytesReturned` and is zero when
no bytes were returned. A nil registry discards observations so optional
instrumentation cannot break a read path.

The registry is accounting only; it does not change scan planning, indexes, or
column reads. Callers decide which read boundaries to report and how to use a
high ratio when tuning a schema or query.
