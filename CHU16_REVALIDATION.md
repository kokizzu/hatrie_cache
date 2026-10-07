# CH-U16 runtime dictionary demotion revalidation

This audit resumes the interrupted 2026-10-07 experiment. It is a revalidation
of existing commit `2c0f8ad0378d4b9365165a5ca6fdaeecc588b869`, not a new feature.
The clean baseline is `ea851bc4dfe266086f15c6731d880fc6c2522f47`.

The shared `master` checkout contains unrelated staged, unstaged, and
untracked work and is not a valid benchmark baseline. All measurements below
run in `/tmp/hatrie-chu16-runtime-dictionary-20261007` through the repository
Makefile and `scripts/codex-chu16-worktree.sh`.

## Regression and harness corrections

- The previous harness ran Go from the shared root. The corrected harness
  changes into the isolated worktree.
- The regression now uses an independent 128-distinct-value boundary rather
  than referring to a constant absent from the baseline implementation.
- `make codex-chu16-test-first` fails behaviorally on the baseline:
  `adaptive dictionary remained enabled after cardinality growth`.
- Test selection now includes the separately named explicit-dictionary
  precedence regression and default/NULL dictionary tests.
- Baseline measurement no longer resets or cleans the worktree.
- Benchmark fixtures are installed separately, before measuring either
  implementation; application no longer reapplies the regression test patch.

## Comparable baseline measurements

Linux/amd64, AMD Ryzen 9 5950X, benchmark suffix `-32`.
`make codex-chu16-benchmark` uses five samples at `-benchtime=200ms` for
steady-state work and five samples at `-benchtime=100x` for transition work.
Both baseline commands passed.

| Workload | Baseline median ns/op | Baseline median B/op | allocs/op |
|---|---:|---:|---:|
| Post-churn updates | 547.9 | 656 | 4 |
| 129-update transition | 82,374 | 95,746 | 801 |

Raw post-churn samples:

```text
ns/op: 629.4 547.9 556.0 509.4 434.4
B/op:  703   635   654   669   656
```

Raw transition samples:

```text
ns/op: 84444 82374 80006 80227 87320
B/op:  95948 95740 95742 95746 95754
```

The original six-layout baseline also passed with five default-duration
samples. Its output is in the continuation transcript. Use the comparable
200ms samples for before/after conclusions; do not mix run durations.

## Candidate results and verification scope

The candidate completed the same benchmark target. Post-churn raw samples:

```text
ns/op: 617.5 538.4 548.2 565.9 507.7
B/op:  689   641   681   660   694
```

Candidate transition samples:

```text
ns/op: 89571 94925 84982 86037 103201
B/op:  100752 100622 100613 100706 100724
```

The candidate medians are 548.2 ns/op and 681 B/op for post-churn updates,
and 89,571 ns/op, 100,706 B/op, 802 allocs/op for the transition. These runs
do not reproduce the old documented speedup: steady-state medians are tied,
and the transition is 8.7% slower with one extra allocation. Fixed-iteration
comparison is pending because benchmark-selected iteration counts differ and
affect cumulative allocation accounting. Do not claim a new performance win.

The fixed-iteration comparison subsequently passed on both implementations:
`make codex-chu16-compare-post-churn` uses 200,000 iterations, seven repeats,
and `-cpu=1` with the same benchmark fixture on separate baseline/candidate
worktrees.

```text
baseline ns/op:  709.7 612.8 613.6 593.8 622.7 640.2 603.5
candidate ns/op: 684.0 563.8 587.2 578.8 601.0 603.0 616.0
```

Both report exactly 624 B/op and 4 allocs/op in every repeat. Medians are
613.6 and 601.0 ns/op (2.1% difference), with substantial overlapping ranges.
This is insufficient evidence for a reliable CPU improvement. The old
12% cumulative-allocation reduction is not reproduced when work counts match.
These allocation measurements do not measure retained dictionary storage.
The previously published opt-in feature is not duplicated or newly promoted
on the strength of these results; this follow-up adds correctness coverage
and records the limitations of the earlier benchmark claim.

- Candidate focused tests passed before adding parity coverage.
- `make codex-chu16-race` passed, including new plain-storage parity coverage
  for append/update churn, NULL/empty values, deletion, and reinsertion.
- `make codex-chu16-vet` passed.
- `make codex-chu16-full-test` failed in
  `TestTypedTableAggregateArrangementCheckpointRestoresGlobalAggregate`,
  `TestTypedTableAggregateArrangementCheckpointIsDeterministic`, and
  `TestTypedTableAggregateArrangementCheckpointRestoresWithoutReplay`.
  `make codex-chu16-baseline-checkpoints` reproduced all three on unchanged
  `ea851bc4` in a separate detached worktree, with `-count=5`: both restore
  tests failed all five times and the nondeterminism test failed three times.
  These failures predate this candidate. Wider compatibility remains
  unverified; the SQL suite must not be reported as passing.
- Reviewed cleanup of this experiment's unused temporary artifacts.

The delivery branch is based directly on existing implementation commit
`2c0f8ad0`; it changes no production code. Its own
`make race-chu16-adaptive-dictionary` and
`make vet-chu16-adaptive-dictionary` both passed. The test and race targets
now include explicit-dictionary precedence, which their previous expression
omitted. Full-package failures above remain explicitly unresolved.
