#!/usr/bin/env bash
set -euo pipefail

go test -tags t210baseline ./hat/hatReplication -run '^$' -bench '^BenchmarkT210BaselineConflictResolve$' -benchmem -count=5
