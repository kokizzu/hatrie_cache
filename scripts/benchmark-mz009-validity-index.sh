#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench 'BenchmarkSQLJSONValidity(Scan|IndexWarm|IndexColdBuild)$' -benchtime=200ms -count=5
