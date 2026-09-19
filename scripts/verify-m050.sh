#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/test-m050-package.sh
bash ./scripts/race-m050.sh
bash ./scripts/vet-m050.sh
