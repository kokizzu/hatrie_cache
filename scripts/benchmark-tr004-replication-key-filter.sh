#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTR004ReplicationKeyPrefixMatcher$' -benchmem -benchtime=200ms -count=5
