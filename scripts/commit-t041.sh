#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/stage-t041.sh
git commit -m "feat: compress archived journal segments"
