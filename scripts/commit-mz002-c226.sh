#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/stage-mz002-c226.sh
git commit -m "integrate typed table change read holds"
