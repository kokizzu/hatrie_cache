#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git diff --cached --stat
git diff --cached --name-only
