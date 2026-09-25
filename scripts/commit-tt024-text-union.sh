#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "feat: wire positional text OR unions [skip ci]"
