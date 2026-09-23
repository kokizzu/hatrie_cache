#!/usr/bin/env bash
set -euo pipefail
bash scripts/format-t228.sh
bash scripts/test-t228.sh
bash scripts/race-t228.sh
bash scripts/vet-t228.sh
