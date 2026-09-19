#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-tu44-memory-stats.sh
go test ./hat/hatMemoryStats
bash scripts/race-tu44-memory-stats.sh
go vet ./hat/hatMemoryStats
