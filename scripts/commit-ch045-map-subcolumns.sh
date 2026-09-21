#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m 'hatSql: cover nested map array paths'
