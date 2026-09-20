#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench '^Benchmark(TU28PoolDialEachCall(NoLifecycle|WithLifecycle)|ConnectionPoolDo|TU28PoolReuseWithLifecycle)$' -benchmem -count=5
