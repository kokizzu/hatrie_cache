#!/usr/bin/env bash
set -euo pipefail

go test -tags mu12 ./hat/hatSql -run '^$' -bench 'BenchmarkMU12(ExplainJSONBaseline|BuildExplainArrangementPlan|MarshalExplainArrangementJSON)' -benchmem -count=5 "$@"
