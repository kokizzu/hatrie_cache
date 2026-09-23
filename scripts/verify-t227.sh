#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-t227.sh
bash scripts/test-t227.sh
bash scripts/race-t227.sh
bash scripts/vet-t227.sh
