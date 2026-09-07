# Delta Joins

`hatSql.DifferentialTemporalJoin` maintains an exact weighted temporal inner
join incrementally. Each side retains rows by identity and maintains an
equality-key arrangement. A positive change probes only the counterpart rows
in its equality group; a negative change uses the key, timestamp, and row
already retained by the join instead of recomputing the source row or
scanning the complete opposite input.

`ApplyLeft` and `ApplyRight` serialize each change batch, preserve deterministic
counterpart order, and emit signed joined-row changes. Multiplicity,
time-distance bounds, underflow, and arithmetic overflow are validated before
the batch changes state. Invalid batches therefore leave the arrangement
unchanged.

This is useful when one input is large and mutations are sparse: maintenance
work is proportional to the matching equality group rather than the complete
large-side row count. `DifferentialTemporalJoin` is independent from the SQL
executor and can be shared by multiple consumers.
