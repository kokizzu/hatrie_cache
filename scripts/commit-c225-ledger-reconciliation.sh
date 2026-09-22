#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "docs: reconcile C225 incremental windows"
