#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench 'Benchmark(KeyWatcherWritePath|KeyWatcherOptionWritePath)' -benchmem -count=5
