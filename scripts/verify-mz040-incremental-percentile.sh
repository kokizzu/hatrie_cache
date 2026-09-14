#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-mz040-incremental-percentile.sh
bash scripts/test-mz040-incremental-percentile.sh
go test ./hat/hatSql -count=1
bash scripts/race-mz040-incremental-percentile.sh
bash scripts/vet-mz040-incremental-percentile.sh
bash scripts/review-mz040-incremental-percentile.sh
