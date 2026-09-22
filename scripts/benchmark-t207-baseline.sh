#!/usr/bin/env bash
set -euo pipefail

go test -tags t207baseline ./hat/hatReplication -run '^$' -bench '^BenchmarkT207Baseline' -benchmem -count=5
