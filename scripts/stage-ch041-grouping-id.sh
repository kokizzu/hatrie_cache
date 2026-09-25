#!/usr/bin/env bash
set -euo pipefail

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-ch041-stage.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

git show HEAD:Makefile > "$tmpdir/Makefile"
cat >> "$tmpdir/Makefile" <<'EOF'

test-ch041-grouping-id:

	bash ./scripts/test-ch041-grouping-id.sh

benchmark-ch041-grouping-id:

	bash ./scripts/benchmark-ch041-grouping-id.sh

format-ch041-grouping-id:

	bash ./scripts/format-ch041-grouping-id.sh

test-ch041-grouping-id-package:

	bash ./scripts/test-ch041-grouping-id-package.sh

race-ch041-grouping-id:

	bash ./scripts/race-ch041-grouping-id.sh

vet-ch041-grouping-id:

	bash ./scripts/vet-ch041-grouping-id.sh

stage-ch041-grouping-id:

	bash ./scripts/stage-ch041-grouping-id.sh

commit-ch041-grouping-id:

	bash ./scripts/commit-ch041-grouping-id.sh

push-ch041-grouping-id:

	bash ./scripts/push-ch041-grouping-id.sh
EOF

git show HEAD:BENCHMARK.md > "$tmpdir/BENCHMARK.md"
cat > "$tmpdir/ch041-section" <<'EOF'
## CH-041 multi-argument `GROUPING_ID`

This focused benchmark compares a derived two-argument grouping bitmask with
the native `GROUPING_ID(region, product)` form over the same four-row,
two-dimensional `CUBE` query. Five `-benchmem` samples ran on the AMD Ryzen 9
5950X through `make benchmark-ch041-grouping-id`.

| Path | Time | Bytes/op | Allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: |
| Derived `GROUPING` arithmetic | 68.779 us | 59,759 | 504 | 1.00x |
| Native `GROUPING_ID` | 45.748 us | 53,973 | 343 | 1.50x faster, 9.7% lower bytes, 31.9% fewer allocations |

The native path is eligible for the existing one-pass grouping executor. The
derived expression is evaluated by the established expanded-branch fallback,
so this comparison includes the benefit of making the bitmask a first-class
projection rather than materializing two separate grouping expressions.

Raw samples:

```text
BenchmarkSQLGroupingIdentifierExisting: 72412 ns/op 59759 B/op 504 allocs/op
BenchmarkSQLGroupingIdentifierExisting: 69739 ns/op 59757 B/op 504 allocs/op
BenchmarkSQLGroupingIdentifierExisting: 64053 ns/op 59759 B/op 504 allocs/op
BenchmarkSQLGroupingIdentifierExisting: 65242 ns/op 59758 B/op 504 allocs/op
BenchmarkSQLGroupingIdentifierExisting: 68779 ns/op 59762 B/op 504 allocs/op
BenchmarkSQLGroupingIdentifierMultiple: 46184 ns/op 53974 B/op 343 allocs/op
BenchmarkSQLGroupingIdentifierMultiple: 45539 ns/op 53973 B/op 343 allocs/op
BenchmarkSQLGroupingIdentifierMultiple: 45748 ns/op 53973 B/op 343 allocs/op
BenchmarkSQLGroupingIdentifierMultiple: 45240 ns/op 53973 B/op 343 allocs/op
BenchmarkSQLGroupingIdentifierMultiple: 46706 ns/op 53973 B/op 343 allocs/op
```
EOF
awk -v section="$tmpdir/ch041-section" '
  $0 == "## TT-050 SQL Planner Statistics" {
    while ((getline line < section) > 0) print line
    close(section)
  }
  { print }
' "$tmpdir/BENCHMARK.md" > "$tmpdir/BENCHMARK.new"
mv "$tmpdir/BENCHMARK.new" "$tmpdir/BENCHMARK.md"

for path in Makefile BENCHMARK.md; do
  blob="$(git hash-object -w "$tmpdir/$path")"
  git update-index --add --cacheinfo "100644,$blob,$path"
done

git add \
  CH041_GROUPING_PLAN_SHARING.md \
  hat/hatSql/ch041_grouping_id_test.go \
  hat/hatSql/ch041_one_pass_grouping.go \
  hat/hatSql/grouping_sets.go \
  scripts/benchmark-ch041-grouping-id.sh \
  scripts/commit-ch041-grouping-id.sh \
  scripts/format-ch041-grouping-id.sh \
  scripts/push-ch041-grouping-id.sh \
  scripts/race-ch041-grouping-id.sh \
  scripts/stage-ch041-grouping-id.sh \
  scripts/test-ch041-grouping-id-package.sh \
  scripts/test-ch041-grouping-id.sh \
  scripts/vet-ch041-grouping-id.sh

git diff --cached --check
