#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -tags mz006baseline -run '^$' -bench '^BenchmarkMZ006NaiveFrontier' -benchmem -count=5
