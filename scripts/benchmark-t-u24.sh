#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^BenchmarkTU24ConditionalIndexMetadata$' -benchmem -count=5
