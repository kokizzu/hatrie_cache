#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/format-tr026-bitmap-index.sh
bash ./scripts/test-tr026-bitmap-index.sh
go test ./hat/hatDataStructure -count=1
bash ./scripts/race-tr026-bitmap-index.sh
bash ./scripts/vet-tr026-bitmap-index.sh
git diff --check
git status --short
