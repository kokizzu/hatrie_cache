#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-mz037-incremental-top-k.sh
bash scripts/test-mz037-incremental-top-k.sh
go test ./hat/hatSql -count=1
bash scripts/race-mz037-incremental-top-k.sh
bash scripts/vet-mz037-incremental-top-k.sh
bash scripts/review-mz037-incremental-top-k.sh
