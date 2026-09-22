#!/usr/bin/env bash
set -euo pipefail

go test -tags t210 ./hat/hatReplication -run '^$' -bench '^BenchmarkT210' -benchmem -count=5
