# T-U34 Per-Space WAL Sync Policy

`CommandJournal` keeps its legacy synchronous durability behavior by default.
Callers that know the logical space for a mutation can opt into a bounded
policy map through `CommandJournalOptions.SpaceSyncPolicies` and use
`ExecuteCommandWithSpace`.

```go
journal, err := hatriecache.OpenCommandJournalWithOptions(path, hatriecache.CommandJournalOptions{
	GroupCommitMaxBatch: 64,
	SpaceSyncPolicies: map[string]hatriecache.CommandJournalSpaceSyncPolicy{
		"critical": {Mode: hatriecache.CommandJournalSpaceSyncSynchronous},
		"reports":  {Mode: hatriecache.CommandJournalSpaceSyncPeriodic, Interval: time.Second},
		"bulk":     {Mode: hatriecache.CommandJournalSpaceSyncDisabled},
	},
})
if err != nil {
	return err
}
response := journal.ExecuteCommandWithSpace(trie, "reports", request)
```

The policy only controls the filesystem sync barrier. Every command is still
written to the same ordered WAL, and the journal record format is unchanged.
Synchronous mode syncs the batch, periodic mode syncs at its configured
interval, and disabled mode leaves the write in the operating-system buffer.
Unknown or empty spaces, the existing `ExecuteCommand` method, and nil policy
maps retain synchronous behavior.

When a group contains multiple spaces, the strongest requirement wins. A
synchronous member forces one sync for the whole ordered batch; a disabled
member can therefore never weaken a critical member. Rollback and close paths
remain synchronous. Policy names are trimmed and bounded, periodic intervals
must be positive, and the policy map is copied during validation.

## Measured Cost

The clean-base journal workload (`BenchmarkTR007GroupCommitFixed`) measured a
five-run median of `2,170,570 ns/op`, `16,249 B/op`, and `90 allocs/op`. The
feature branch measured `2,171,869 ns/op`, `16,259 B/op`, and `90 allocs/op`
for the same benchmark: `1.00x` within normal filesystem noise, with no
allocation increase.

The decision-only benchmarks measured:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Legacy nil-policy fast path | 2.166 | 0 | 0 |
| Disabled policy lookup | 12.64 | 0 | 0 |
| Periodic policy lookup | 13.94 | 0 | 0 |

The policy is opt-in. The default path does not allocate or consult a policy
map, and the durability reduction is explicit for each named space.
