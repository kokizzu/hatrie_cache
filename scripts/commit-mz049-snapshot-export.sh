#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git commit -m "hatCache: add resumable snapshot export"
