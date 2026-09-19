#!/usr/bin/env bash
set -euo pipefail

go test -tags mu12baseline ./hat/hatSql -run '^$' -bench 'BenchmarkMU12ExplainJSONBaseline' -benchmem -count=5 "$@"
