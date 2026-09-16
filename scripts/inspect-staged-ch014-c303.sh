#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git diff --cached --name-status
git diff --cached --stat
