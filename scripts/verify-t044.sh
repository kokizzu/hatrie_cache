#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/test-t044-package.sh
bash ./scripts/race-t044.sh
bash ./scripts/vet-t044.sh
