#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkHTTPReplicatorAsyncPauseResume$' -benchmem -count=5
