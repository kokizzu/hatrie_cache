#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-t226.sh
bash scripts/test-t226.sh
bash scripts/race-t226.sh
bash scripts/vet-t226.sh
