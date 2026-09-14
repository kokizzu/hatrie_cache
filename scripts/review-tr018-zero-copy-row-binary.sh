#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/format-tr018-zero-copy-row-binary.sh
bash ./scripts/test-tr018-zero-copy-row-binary.sh
bash ./scripts/race-tr018-zero-copy-row-binary.sh
bash ./scripts/vet-tr018-zero-copy-row-binary.sh
git diff --check
git status --short
