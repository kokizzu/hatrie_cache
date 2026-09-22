#!/usr/bin/env bash
set -euo pipefail

go test -tags t208baseline ./hat/hatReplication -run '^$' -bench '^BenchmarkT208Baseline' -benchmem -count=5
