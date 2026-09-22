#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "M239 explain temporal requirements"
