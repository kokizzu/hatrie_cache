#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/stage-t069.sh
git commit -m "feat: compare restore rehearsal state checksums"
