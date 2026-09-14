#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-mz029-incremental-interval-join.sh
bash scripts/test-mz029-incremental-interval-join.sh
go test ./hat/hatSql -count=1
bash scripts/race-mz029-incremental-interval-join.sh
bash scripts/vet-mz029-incremental-interval-join.sh
bash scripts/review-mz029-incremental-interval-join.sh
