#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCHU34HTTPMutationRequest$' -benchmem -count=5 -benchtime=250ms
