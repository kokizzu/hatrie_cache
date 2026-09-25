#!/usr/bin/env bash
set -euo pipefail
bash scripts/stage-m052aa-native-join.sh verify
git diff --cached --check
git commit -m "feat: add automatic native equality hash joins"
