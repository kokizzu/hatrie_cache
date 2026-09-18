#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "feat: derive sink idempotency tokens [skip ci]"
