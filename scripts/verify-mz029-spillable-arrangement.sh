#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-mz029-spillable-arrangement.sh
bash scripts/test-mz029-spillable-arrangement.sh
go test ./hat/hatDataStructure -count=1
bash scripts/race-mz029-spillable-arrangement.sh
bash scripts/vet-mz029-spillable-arrangement.sh
bash scripts/review-mz029-spillable-arrangement.sh
