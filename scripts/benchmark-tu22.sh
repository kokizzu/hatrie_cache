#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTU22CrossIndexUniqueSet' -benchmem -count=5
