#!/usr/bin/env bash
set -euo pipefail

go test -tags mu37 ./hat/hatStorage -run '^$' -bench '^BenchmarkMU37CompactionDiagnostics' -benchmem -count=5
