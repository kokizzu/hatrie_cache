#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m 'Optimize monotonic posting insertion'
