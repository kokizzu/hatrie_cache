#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "Add timeline recovery reconciliation [skip ci]"
