#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTT016PersistentStoreSave$' -benchmem -benchtime=1s -count=5
