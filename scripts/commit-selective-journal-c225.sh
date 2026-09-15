#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m 'Allow checkpoint-only journals in selective restore'
