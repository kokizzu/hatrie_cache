#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m 'feat: add typed table column TTL [skip ci]'
