#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-mz039-incremental-distinct.sh
bash scripts/test-mz039-incremental-distinct.sh
go test ./hat/hatSql -count=1
bash scripts/race-mz039-incremental-distinct.sh
bash scripts/vet-mz039-incremental-distinct.sh
bash scripts/review-mz039-incremental-distinct.sh
