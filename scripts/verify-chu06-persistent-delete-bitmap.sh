#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-chu06-persistent-delete-bitmap.sh
bash scripts/test-chu06-persistent-delete-bitmap.sh
bash scripts/race-chu06-persistent-delete-bitmap.sh
bash scripts/vet-chu06-persistent-delete-bitmap.sh
