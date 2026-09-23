#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-t229.sh
bash scripts/test-t229.sh
bash scripts/race-t229.sh
bash scripts/vet-t229.sh
