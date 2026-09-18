#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "Add arrangement locality hints [skip ci]"
