# T-042: Recovery Parallel Replay Evaluation

Status: rejected and rolled back.

The experiment added an opt-in replay mode with four workers. It grouped
independent key-local `SETSTR` mutations and preserved ordering within each
key. The existing serial replay API was unchanged, and unsupported commands
fell back to the serial path.

The benchmark used 8,320 journal mutations across 128 independent keys. The
serial baseline was measured with five samples; the parallel result below is
the first completed sample because it already showed a decisive regression.

| Path | ns/op | B/op | allocs/op | Relative to serial |
| --- | ---: | ---: | ---: | --- |
| Serial replay, median of five | 7,949,797 | 2,175,294 | 40,987 | 1.00x |
| Four-worker replay, first sample | 14,445,445 | 13,805,852 | 41,945 | 1.82x slower, 6.35x bytes, 2.3% more allocations |

The added grouping/index buffers and contention on the shared trie lock cost
more than the parallel workers saved. The API and implementation were removed;
the repository retains the existing serial replay path and no new recovery
behavior was shipped.

The next viable attempt would need partition-local state or a trie write path
that avoids global lock contention, with correctness and memory measured before
reintroducing parallel replay.
