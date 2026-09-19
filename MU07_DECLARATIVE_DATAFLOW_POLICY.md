# M-U07 Declarative Differential Dataflow Policy

`hatSql.DifferentialDataflow` couples late-data admission, correction mode,
frontier advancement, and operator statistics around one maintained sink. It
is an opt-in policy layer for differential operators such as
`DifferentialWindow`; existing `DifferentialWatermark` and batch filtering
APIs remain unchanged.

## Policy

`DifferentialDataflowPolicy` has three fields:

- `AllowedLateness` is a non-negative logical timestamp distance. An update at
  `frontier - AllowedLateness` is accepted; an update older than that is
  too-late.
- `Correction` chooses `DifferentialDataflowAccept`, `Drop`, or `Reject` for
  too-late updates. Updates inside the lateness bound are always accepted.
- `Frontier` is `DifferentialDataflowManual` or
  `DifferentialDataflowBatchMax`. Manual mode advances only through
  `Advance`. Batch-max mode also advances to the greatest timestamp in each
  successful batch. `Advance` is valid in either mode and is monotonic.

The zero-value policy is conservative about time ordering: no lateness is
allowed and the frontier is manual. The flow itself is opt-in, and its zero
correction mode is `Accept` so correctness is preserved unless the caller
explicitly chooses to drop or reject corrections.

## Maintained sink example

The sink must be batch-atomic. A sink error means that the accepted batch and
automatic frontier advancement are not committed.

```go
window, err := hatSql.NewDifferentialWindow(hatSql.DifferentialWindowOptions{
	Mode:  hatSql.DifferentialWindowFrameRows,
	Start: -1,
	End:   0,
})
if err != nil {
	return err
}

flow, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{
	Policy: hatSql.DifferentialDataflowPolicy{
		AllowedLateness: 2,
		Correction:      hatSql.DifferentialDataflowDrop,
		Frontier:        hatSql.DifferentialDataflowManual,
	},
	InitialFrontier: 100,
	Sink: func(rows []hatSql.DifferentialRow) error {
		_, err := window.Apply(rows)
		return err
	},
})
if err != nil {
	return err
}

if err := flow.Apply([]hatSql.DifferentialRow{
	{Key: "on-time", Time: 100, Diff: 1},
	{Key: "within-bound", Time: 98, Diff: 1},
	{Key: "too-late", Time: 97, Diff: 1},
}); err != nil {
	return err
}
stats := flow.Stats()
```

The first two rows reach the window. The third is dropped because its
lateness is three logical units, greater than the configured bound of two.
With `Reject`, the complete batch would be rejected and the sink would not be
called. With `Accept`, all three rows would reach the window as corrections.

## Operator reporting

`Stats` returns an independent snapshot containing:

- current `Frontier`;
- successful `AppliedBatches` and `AcceptedRows`;
- `LateRows`, `AcceptedLateRows`, and `TooLateRows`;
- `DroppedRows`, `RejectedBatches`, and `RejectedRows`;
- `SinkErrors`.

Rejected batches update rejection counters but do not update accepted state or
the frontier. Sink failures increment `SinkErrors` and leave the batch and
frontier uncommitted. Accepted rows are detached clones, so a sink cannot
mutate caller-owned input maps.

## Tradeoffs and limits

The policy gate is an O(batch size) pass and keeps one cloned accepted slice.
It does not retain old rows or perform compaction; the maintained sink owns
that state and should use its own retention or frontier policy. Batch-max is
convenient for append-oriented sources but must not be used when the observed
maximum timestamp proves no completeness guarantee. Manual mode is the
appropriate choice when a source provides an independent watermark.

The policy layer adds no wire or storage format and does not change the
existing SQL execution path. It is intended to sit at a source/operator
boundary where a caller can make the sink atomic.

## Verification and measurements

```sh
make test-mu07
make verify-mu07
make benchmark-mu07-baseline
make benchmark-mu07
```

The focused tests cover bounded late rows, all correction modes, manual and
batch-max frontiers, regression rejection, atomic policy rejection, sink
failure rollback, row detachment, empty batches, and a real
`DifferentialWindow` sink.

On an AMD Ryzen 9 5950X with 1,024 updates and `-count=5 -benchtime=100ms`,
the existing late-data filter had median `138,041 ns/op`, `212,993 B/op`, and
`1,025 allocs/op`. The new manual policy path measured `134,965 ns/op`,
`212,993 B/op`, and `1,025 allocs/op`, so the policy/reporting layer did not
add measurable allocation or memory cost in this fixture. Batch-max measured
`13,865 ns/op`, `41,653 B/op`, and `5 allocs/op` after its frontier had
advanced, because most repeated rows were then classified as too late and
dropped. That batch-max number is not a semantic replacement for the manual
control; it demonstrates the intended frontier-pruning behavior.
