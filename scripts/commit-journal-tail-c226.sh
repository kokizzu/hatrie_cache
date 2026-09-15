#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m 'Filter safe journal tails in selective restore'
