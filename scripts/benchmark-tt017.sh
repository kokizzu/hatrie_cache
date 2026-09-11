#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTT017PersistentStoreEntry$' -benchmem -benchtime=1s -count=5
