#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat
git status --short
