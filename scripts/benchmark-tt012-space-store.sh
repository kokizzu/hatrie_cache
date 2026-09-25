#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkTT012PersistentSpaceStoreLookup$' -benchtime=200ms -benchmem -count=5
