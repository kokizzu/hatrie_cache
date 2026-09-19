#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu45 ./hat/hatSql -run '^$' -bench '^Benchmark(SelectTypedTableJoinArrangementOneAlternative|SelectTypedTableJoinArrangementThreeAlternatives)$' -benchmem -count=5
