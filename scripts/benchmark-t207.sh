#!/usr/bin/env bash
set -euo pipefail

go test -tags t207 ./hat/hatReplication -run '^$' -bench '^BenchmarkT207' -benchmem -count=5
