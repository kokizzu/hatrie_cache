#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-mz030-incremental-join.sh
bash scripts/test-mz030-incremental-join.sh
go test ./hat/hatSql -count=1
bash scripts/race-mz030-incremental-join.sh
bash scripts/vet-mz030-incremental-join.sh
bash scripts/review-mz030-incremental-join.sh
