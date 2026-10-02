#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench 'BenchmarkConnectionPoolDo$|BenchmarkConnectionPoolDoWithLifecycleHooks$' -benchmem -count=5 -benchtime=200ms
