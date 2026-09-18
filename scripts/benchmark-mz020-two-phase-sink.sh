#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkMZ020(OnePhaseBaseline|TwoPhaseCoordinator)$' -benchmem -benchtime=500ms -count=5
