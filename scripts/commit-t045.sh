#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/stage-t045.sh
git commit -m "test: add journal crash fault matrix"
