#!/usr/bin/env bash
set -euo pipefail

go test -tags t208 ./hat/hatReplication -run '^$' -bench '^BenchmarkT208' -benchmem -count=5
