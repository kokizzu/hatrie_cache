#!/usr/bin/env bash
set -euo pipefail

go test -tags mu11 ./hat/hatSql -run '^$' -bench 'Benchmark(TypedTable.*ArrangementSnapshot|MU11.*ArrangementAdvisor)' -benchmem -count=5 "$@"
