#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "feat(topology): add bounded bucket migration coordination"
