#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "Explain projection selection and I/O"
