#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-mz031-skew-aware-join-exchange.sh
bash scripts/test-mz031-skew-aware-join-exchange.sh
go test ./hat/hatSql -count=1
bash scripts/race-mz031-skew-aware-join-exchange.sh
bash scripts/vet-mz031-skew-aware-join-exchange.sh
bash scripts/review-mz031-skew-aware-join-exchange.sh
