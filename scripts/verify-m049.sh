#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/test-m049-package.sh
bash ./scripts/race-m049.sh
bash ./scripts/vet-m049.sh
