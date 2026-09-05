#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/stage-t147.sh
git commit -m "feat: add structured command error codes"
