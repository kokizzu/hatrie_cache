#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkMZ005' -benchmem -count=5 -benchtime=500ms
