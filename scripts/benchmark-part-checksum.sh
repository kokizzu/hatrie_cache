#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatMerkle -run '^$' -bench '^BenchmarkChecksumPart$' -benchmem -count=5
