#!/usr/bin/env bash
set -euo pipefail

git status --short
git log -1 --oneline
