#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-t230.sh
bash scripts/test-t230.sh
bash scripts/race-t230.sh
bash scripts/vet-t230.sh
