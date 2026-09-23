#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m 'feat: default archived WAL segments to bounded zstd'
