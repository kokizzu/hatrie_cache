#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-t231.sh
bash scripts/test-t231.sh
bash scripts/race-t231.sh
bash scripts/vet-t231.sh
