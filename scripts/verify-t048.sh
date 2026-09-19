#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/test-t048-package.sh
bash ./scripts/race-t048.sh
bash ./scripts/vet-t048.sh
