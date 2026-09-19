#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/test-mu048-package.sh
bash ./scripts/race-mu048.sh
bash ./scripts/vet-mu048.sh
