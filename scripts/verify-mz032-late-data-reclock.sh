#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-mz032-late-data-reclock.sh
bash scripts/test-mz032-late-data-reclock.sh
bash scripts/race-mz032-late-data-reclock.sh
bash scripts/vet-mz032-late-data-reclock.sh
bash scripts/review-mz032-late-data-reclock.sh
