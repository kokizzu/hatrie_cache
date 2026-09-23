# TT-G10 WAL Segment Seek

Segmented command journals already encode each archived segment's first and
last mutation sequence in its filename. Replay now uses those bounds as a
range index: a replay beginning at `afterSequence` skips archived segments
whose end sequence is at or before that point, and a bounded replay also
stops before segments that start after `targetSequence`.

The journal format, segment rotation, checksums, encryption, and command
application are unchanged. Full replay (`afterSequence == 0`) and
unsegmented journals retain the existing scan. Journal opening still performs
its normal complete validation; the optimization only avoids reopening and
decoding segments that cannot contribute to the requested replay.

The focused regression test compares ranged replay with a full-scan reference
and verifies that a deliberately corrupted old segment is not read by a
ranged scan. `ReplayThrough` uses the same bounded path.

## Measurement

Workload: 4,096 binary `SETSTR` entries, 512-byte segment target, 1,024
retained segments, replay starting at sequence 3,968. Five benchmark samples
were run through `make m229-tt-g10-benchmark` on an AMD Ryzen 9 5950X.

| Path | Median ns/op | Median B/op | Median allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: |
| Existing full archived-segment scan | 10,842,900 | 3,699,407 | 28,234 | baseline |
| Range-indexed segment seek | 1,065,587 | 344,563 | 2,702 | 10.18x faster, 10.74x lower heap, 10.45x fewer allocations |

The tradeoff is limited to the metadata slice needed to list segment names;
it remains proportional to the number of retained segments. Full replay still
has full-scan cost, and a replay that starts near the journal head cannot skip
much work.
