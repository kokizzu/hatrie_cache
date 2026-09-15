#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "Add SQL aggregate state combinators"
